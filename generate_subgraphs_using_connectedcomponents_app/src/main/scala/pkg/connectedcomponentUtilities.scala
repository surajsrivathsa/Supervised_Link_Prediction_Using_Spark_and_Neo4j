

package pkg

import org.apache.spark.RangePartitioner
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, collect_list}
import org.apache.spark.storage.StorageLevel
import org.graphframes.GraphFrame

import scala.collection.mutable

class connectedcomponentUtilities(spark: SparkSession) {
  import spark.implicits._

  def createCompleteGraphFromFile(const: constantsFile): GraphFrame =
  {
    val startTime = System.currentTimeMillis()

    val tmp1 = spark.read.option("inferSchema", "true").parquet(const.srcFolderPath + const.vertexFolder + "/*");
    var vertexdf = tmp1.withColumnRenamed("authorId","id").withColumnRenamed("authorLabel","label")

    vertexdf.show(10);

    val edgesdf = spark.read.schema(const.edgesSchema).option("inferSchema", "false").parquet(const.srcFolderPath + const.edgesFolder + "/*");

    edgesdf.show(10);
    vertexdf.persist(StorageLevel.MEMORY_AND_DISK_SER);
    //edgesdf.persist(StorageLevel.DISK_ONLY);

    val graph = GraphFrame(vertexdf, edgesdf);

    //graph.inDegrees.show(10);
    //println("Count of edges is : " + edgesdf.count())
    val stopTime = System.currentTimeMillis();

    println("Time taken to form graph is: " + (stopTime-startTime) )

    return graph
  }


  def generateCliques(const: constantsFile, graph: GraphFrame): RDD[List[Long]] = {

    val startTime = System.currentTimeMillis()
    val bidirectional_edges = graph.edges

    val tmp1 = bidirectional_edges.groupBy("paperId", "year").agg(collect_list(col("src")).as("combined_cols"))

    val removeDuplicates: mutable.WrappedArray[Long] => mutable.WrappedArray[Long] = _.distinct
    val udfremoveDuplicates = spark.udf.register("removeDuplicates", removeDuplicates)

    val tmp2 = tmp1.withColumn("cliques", udfremoveDuplicates(col("combined_cols"))).select("paperId","year","cliques")
    tmp2.show(10);

    val tmp3 = tmp2.rdd.map(row => (row(0) ,row(1) , row(2).asInstanceOf[mutable.WrappedArray[Long]].mkString(",")))
    tmp3.take(10).foreach(println)

    val cliques = tmp3.map(x => {
      val nodes =  x._3.split(",").map(y => y.toLong).toList
      nodes
    })

    cliques.persist(StorageLevel.DISK_ONLY)

    val stopTime = System.currentTimeMillis()

    println("Time taken to generate clique file in seconds: " + (stopTime-startTime)/1000)

    return cliques
  }


  def runConnectedComponents(const: constantsFile,cliques: RDD[List[Long]]): Unit= {
    val startTime = System.currentTimeMillis()

    val (cc, didConverge, iterCount) = ConnectedComponent.run(cliques, const.convergence_iteration)

    if (didConverge) {
      println("Converged in " + iterCount + " iterations")
      val cc2 = cc.map(x => {
        (x._2, List(x._1))
      })

      /**
       * Get all the nodes in the connected component/
       * We are using a rangePartitioner because the CliquesGenerator produces data skew.
       */
      val rangePartitioner = new RangePartitioner(cc2.getNumPartitions, cc2)
      val connectedComponents =  cc2.reduceByKey(rangePartitioner, (a, b) => {b ::: a})

      //connectedComponents.mapPartitionsWithIndex((index, iter) => {
      //  iter.toList.map(x => (index, x._1, x._2.size)).iterator
      //  }).collect.foreach(println)

      println("count of connected components: " + connectedComponents.count())
      connectedComponents.map(x => (x._2.length).toString + "|" + x._1 + "|" + x._2.sorted.mkString(",")).take(10).foreach(println)
      connectedComponents.map(x => (x._2.length).toString + "|" + x._1 + "|" + x._2.sorted.mkString(",")).saveAsTextFile(const.srcFolderPath+const.connectedcomponentsFolder)
    }
    else {
      println("Max iteration reached.  Could not converge")
    }

    val stopTime = System.currentTimeMillis()

    println("Time taken for connected components in seconds: " + (stopTime-startTime)/1000)

    return ;
  }


  def createDfFromCC2File(const: constantsFile): DataFrame = {
    val inputFilerdd = spark.sparkContext.textFile(const.srcFolderPath+const.connectedcomponentsFolder + "/*")

    //parse the file and convert the data to three columns column1 : length, column2: least vertexid in connected component, col3: all connected components in an array
    // example: (4, 23, [23, 45, 56,123] :: also take only some connected components as we will use subgraphs in all our further pursuits
 //handle the disparate graph
    val splitrdd = inputFilerdd.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }.
      map({ line =>
        var row = line.split("\\|");
        (row(0).toInt, row(1).toLong, row(2).toString.split(",").map(_.toLong).distinct)
      }).
      filter(row => row._1 < const.maxcomponentsize).
      take(const.maxcomponentrecords)

    /*
    //for taking some pieces from connected components
    val splitrdd1 = inputFilerdd.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }.
      map({ line =>
        var row = line.split("\\|");
        (row(0).toInt, row(1).toLong, row(2).toString.split(",").map(_.toLong).distinct)
      }).
      filter(row => row._1 < 0).
      take(const.maxcomponentrecords1)

     */

    /*
    flatten the above example, if the length is 4 then the output will be 4 records
    4, 23, 23
    4, 23, 45
    4, 23, 56
    4, 23, 123
     */

    //val splitrdd = splitrdd0.union(splitrdd1)

    val flattendrdd = splitrdd.flatMap{case (k1, k2 ,v) => v.map(vx => (k1, k2 ,vx))}

    //convert RDD to dataframe as it is needed to filter out edges from complete graph

    val flattendDF = spark.sparkContext.parallelize(flattendrdd).toDF("length", "least_vertex_id", "connected_vertices")
    flattendDF.persist(StorageLevel.DISK_ONLY)
    println("Count of the flattend dataframe is: " + flattendDF.count())
    flattendDF.show(10)

    flattendDF.write.mode("overwrite").parquet(const.srcFolderPath + const.flatteneddfFolder)

    return flattendDF

  }


  def createSubGraphsFromCC2andCompleteGraph(const: constantsFile, graph: GraphFrame, cc2df: DataFrame): Unit = {

    graph.vertices.createOrReplaceGlobalTempView("v")
    graph.edges.createOrReplaceGlobalTempView("e")
    cc2df.createOrReplaceGlobalTempView("c")

    //Filter out the required edges and vertices only from the completegraph using our connected components.
    //Take distinct to avoid any duplicates
    var tmpedges = spark.sql("SELECT /*+ MAPJOIN(c) */ DISTINCT e.* FROM global_temp.e as e inner join global_temp.c on e.src = c.connected_vertices")
    tmpedges.createOrReplaceGlobalTempView("t1");

    var subsetEdges = spark.sql("SELECT /*+ MAPJOIN(c) */ DISTINCT e.* FROM global_temp.t1 as e inner join global_temp.c on e.dst = c.connected_vertices")

    var subsetVertices = spark.sql("SELECT /*+ MAPJOIN(c) */ DISTINCT v.* from global_temp.v as v inner join global_temp.c on v.id = c.connected_vertices")

    //subsetEdges.persist(StorageLevel.DISK_ONLY)
    //subsetVertices.persist(StorageLevel.DISK_ONLY)
    //subsetEdges.show(10);
    //subsetVertices.show(10);

    //println("count of subset edges is: " + subsetEdges.count());
    //println("count of subset vertices is: " +subsetVertices.count());
    //Write the dataframes into a edge and vertex files


    subsetEdges.write.option("spark.sql.parquet.compression.codec","snappy").mode("overwrite").parquet(const.srcFolderPath+const.subgraphedgesFolder)
    subsetVertices.write.option("spark.sql.parquet.compression.codec","snappy").mode("overwrite").parquet(const.srcFolderPath+const.subgraphvertexFolder)

    println("Count of edges: " + spark.read.parquet(const.srcFolderPath+const.subgraphedgesFolder + "/*").count())
    println("Count of vertices: " + spark.read.parquet(const.srcFolderPath+const.subgraphvertexFolder + "/*").count())

  }


  def readFlattenedDF(const: constantsFile): DataFrame = {
    val tmp1: DataFrame = spark.read.option("inferSchema", "true").parquet(const.srcFolderPath + const.flatteneddfFolder + "/*");
    tmp1.createOrReplaceGlobalTempView("t1");
    tmp1.persist()
    println("Count of records: " + tmp1.count())
    return tmp1

  }



}
