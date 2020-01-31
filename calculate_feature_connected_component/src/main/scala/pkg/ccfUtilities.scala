package pkg

import org.apache.spark.RangePartitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, collect_list}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.graphframes.GraphFrame

import scala.collection.mutable

class ccfUtilities(spark: SparkSession) {

  import spark.implicits._

  def generateSubgraphsFromFile(const: constantsFile): Array[GraphFrame] =
  {
    val startTime = System.currentTimeMillis()

    val trainingvertexdf = spark.read.schema(const.vertexSchema).option("inferSchema", "false").parquet(const.dstFolderPath + const.trainingsubgraphverticesFolder + "/*");

    val testingvertexdf = spark.read.schema(const.vertexSchema).option("inferSchema", "false").parquet(const.dstFolderPath + const.testingsubgraphverticesFolder + "/*");


    trainingvertexdf.show(10);
    testingvertexdf.show(10);

    val trainingedgesdf = spark.read.schema(const.edgesSchema).option("inferSchema", "false").parquet(const.dstFolderPath + const.trainingsubgraphedgesFolder + "/*");
    val testingedgesdf = spark.read.schema(const.edgesSchema).option("inferSchema", "false").parquet(const.dstFolderPath + const.testingsubgraphedgesFolder + "/*");

    trainingedgesdf.show(10);
    testingedgesdf.show(10);

    trainingvertexdf.persist(StorageLevel.MEMORY_AND_DISK_SER);
    trainingedgesdf.persist(StorageLevel.DISK_ONLY);

    testingvertexdf.persist(StorageLevel.MEMORY_AND_DISK_SER);
    testingedgesdf.persist(StorageLevel.DISK_ONLY);

    val trainingGraph = GraphFrame(trainingvertexdf, trainingedgesdf);
    val testingGraph = GraphFrame(testingvertexdf, testingedgesdf);

    trainingGraph.inDegrees.show(10);
    testingGraph.inDegrees.show(10);

    val stopTime = System.currentTimeMillis();

    println("Time taken to form graph is: " + (stopTime-startTime) )

    return Array(trainingGraph,testingGraph)
  }

  def generateSamplesFromFile(const: constantsFile): Array[Dataset[Row]] = {

    val trainingSamples = spark.read.schema(const.authorTupleSchema).option("sep", "|").option("header", "true").option("inferSchema", "false").csv(const.dstFolderPath+const.trainingsamplesFolder + "/*");
    val testingSamples = spark.read.schema(const.authorTupleSchema).option("sep", "|").option("header", "true").option("inferSchema", "false").csv(const.dstFolderPath+const.testingsamplesFolder + "/*");
    trainingSamples.show(10); testingSamples.show(10);

    return Array(trainingSamples, testingSamples)
  }


  def readFlattenedDF(const: constantsFile): DataFrame = {
    val tmp1: DataFrame = spark.read.option("inferSchema", "true").parquet(const.srcFolderPath + const.flatteneddfFolder + "/*");
    tmp1.createOrReplaceGlobalTempView("t1");
    tmp1.persist()
    println("Count of records: " + tmp1.count())
    return tmp1

  }

  def readFlattenedTrainingDF(const: constantsFile): DataFrame = {
    val tmp1: DataFrame = spark.read.option("inferSchema", "true").parquet(const.srcFolderPath + const.flattenedtrainingdfFolder + "/*");
    tmp1.createOrReplaceGlobalTempView("t1");
    tmp1.persist()
    println("Count of records: " + tmp1.count())
    return tmp1

  }

  def readFlattenedTestingDF(const: constantsFile): DataFrame = {
    val tmp1: DataFrame = spark.read.option("inferSchema", "true").parquet(const.srcFolderPath + const.flattenedtestingdfFolder + "/*");
    tmp1.createOrReplaceGlobalTempView("t1");
    tmp1.persist()
    println("Count of records: " + tmp1.count())
    return tmp1

  }

  def createccfTraining(const: constantsFile, cc : DataFrame, samples: DataFrame): Unit = {

    var startTime = System.currentTimeMillis();
    cc.createOrReplaceGlobalTempView("c");
    samples.createOrReplaceGlobalTempView("s");

    var tmp1 = spark.sql("SELECT s.*, c.least_vertex_id as lv1 FROM global_temp.c as c inner join global_temp.s as s on c.connected_vertices = s.authorId1")
    tmp1.createOrReplaceGlobalTempView("t1");

    var tmp2 = spark.sql("SELECT s.*, c.least_vertex_id as lv2 FROM global_temp.c as c inner join global_temp.t1 as s on c.connected_vertices = s.authorId2")
    tmp2.createOrReplaceGlobalTempView("t2");
    //tmp1.show(20);
    tmp2.show(20);
    println("count of records: " + tmp2.count())

    var finaloutput = spark.sql("SELECT s.authorId1 as authorId1, s.authorId2 as authorId2, (CASE WHEN s.lv1 = s.lv2 THEN 1 ELSE 0 END) AS connected_component from global_temp.t2 as s");
    finaloutput.write.option("sep","|").mode("overwrite").csv(const.dstFolderPath+const.ccftrainingFolder)
    var stopTime = System.currentTimeMillis();
    finaloutput.show(10);
    println("Final training dataset count: " + finaloutput.count());
    println("Time taken in seconds: " + (stopTime-startTime)/1000);

  }

  def createccfTesting(const: constantsFile, cc : DataFrame, samples: DataFrame): Unit = {

    var startTime = System.currentTimeMillis();
    cc.createOrReplaceGlobalTempView("c");
    samples.createOrReplaceGlobalTempView("s");

    var tmp1 = spark.sql("SELECT s.*, c.least_vertex_id as lv1 FROM global_temp.c as c inner join global_temp.s as s on c.connected_vertices = s.authorId1")
    tmp1.createOrReplaceGlobalTempView("t1");

    var tmp2 = spark.sql("SELECT s.*, c.least_vertex_id as lv2 FROM global_temp.c as c inner join global_temp.t1 as s on c.connected_vertices = s.authorId2")
    tmp2.createOrReplaceGlobalTempView("t2");
    //tmp1.show(20);
    tmp2.show(20);
    println("count of records: " + tmp2.count())

    var finaloutput = spark.sql("SELECT s.authorId1 as authorId1, s.authorId2 as authorId2, (CASE WHEN s.lv1 = s.lv2 THEN 1 ELSE 0 END) AS connected_component from global_temp.t2 as s");
    finaloutput.write.option("sep","|").mode("overwrite").csv(const.dstFolderPath+const.ccftestingFolder)
    var stopTime = System.currentTimeMillis();
    finaloutput.show(10);
    println("Final training dataset count: " + finaloutput.count());
    println("Time taken in seconds: " + (stopTime-startTime)/1000);

  }


  def generateTrainingCliques(const: constantsFile, graph: GraphFrame): RDD[List[Long]] = {

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

  def generateTestingCliques(const: constantsFile, graph: GraphFrame): RDD[List[Long]] = {

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


  def runConnectedComponentsForTraining(const: constantsFile,cliques: RDD[List[Long]]): Unit= {
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
      connectedComponents.map(x => (x._2.length).toString + "|" + x._1 + "|" + x._2.sorted.mkString(",")).saveAsTextFile(const.srcFolderPath+const.connectedcomponentstrainingFolder)
    }
    else {
      println("Max iteration reached.  Could not converge")
    }

    val stopTime = System.currentTimeMillis()

    println("Time taken for connected components in seconds: " + (stopTime-startTime)/1000)

    return ;
  }

  def runConnectedComponentsForTesting(const: constantsFile,cliques: RDD[List[Long]]): Unit= {
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
      connectedComponents.map(x => (x._2.length).toString + "|" + x._1 + "|" + x._2.sorted.mkString(",")).saveAsTextFile(const.srcFolderPath+const.connectedcomponentstestingFolder)
    }
    else {
      println("Max iteration reached.  Could not converge")
    }

    val stopTime = System.currentTimeMillis()

    println("Time taken for connected components in seconds: " + (stopTime-startTime)/1000)

    return ;
  }


  def createtrainingDfFromCC2File(const: constantsFile): DataFrame = {
    val inputFilerdd = spark.sparkContext.textFile(const.srcFolderPath+const.connectedcomponentstrainingFolder + "/*")

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

    flattendDF.write.mode("overwrite").parquet(const.srcFolderPath + const.flattenedtrainingdfFolder)

    return flattendDF

  }

  def createtestingDfFromCC2File(const: constantsFile): DataFrame = {
    val inputFilerdd = spark.sparkContext.textFile(const.srcFolderPath+const.connectedcomponentstestingFolder + "/*")

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

    flattendDF.write.mode("overwrite").parquet(const.srcFolderPath + const.flattenedtestingdfFolder)

    return flattendDF

  }




}
