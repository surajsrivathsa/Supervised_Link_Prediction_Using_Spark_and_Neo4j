package pkg

import org.apache.spark.graphx.PartitionStrategy.EdgePartition2D
import org.apache.spark.graphx.VertexId
import org.apache.spark.graphx.lib.ShortestPaths
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.graphframes.GraphFrame

import scala.collection.mutable.ListBuffer

class fspUtilities(spark: SparkSession) {

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


  def generateFirstShortestPath(trainingGraph: GraphFrame, testingGraph: GraphFrame, trainingSamples: Dataset[Row],testingSamples: Dataset[Row], const: constantsFile): Unit = {

    val trainingSamplesSources = trainingSamples.rdd.map(line => line(0).asInstanceOf[VertexId]).distinct().collect().toList;

    val testingSamplesSources = testingSamples.rdd.map(line => line(0).asInstanceOf[VertexId]).distinct().collect().toList;

    val trainingGraph_gx = trainingGraph.toGraphX.partitionBy(EdgePartition2D)
    val testingGraph_gx = testingGraph.toGraphX.partitionBy(EdgePartition2D)

    var trainingStartTime = System.currentTimeMillis()

    /*
    Spark doesn't have api to run shortestpath between two vertices. instead it can give ypu all the available paths from a single vertex.
    Hence make source vertex as landmark and tryto find all the vertices that have paths to the landmark and ther shortestpath
    Later Just filter out this with the target id to get required distance, If there is no target id then there is no path hence default to zero
     */

    val result_training = ShortestPaths.run(trainingGraph_gx, trainingSamplesSources)

    val shortestPaths_training = result_training.vertices.filter(line => line._2.size > 0).
      flatMap({case (k,v) => v.map(vx => (k,vx))}).
      map(line => (line._1, line._2._1, line._2._2)).
      filter(line => line._1 != line._2)

    val shortestPaths_training_df = shortestPaths_training.toDF("authorId1", "authorId2","distance")

    shortestPaths_training_df.persist(StorageLevel.MEMORY_AND_DISK_SER)
    shortestPaths_training_df.where("distance > 0").show(10);
    println("Count of training first shortest path records: " + shortestPaths_training_df.count())

    shortestPaths_training_df.write.option("sep","|").mode("overwrite").csv(const.dstFolderPath+const.trainingfspFolder);
    shortestPaths_training_df.unpersist();


    var trainingstopTime = System.currentTimeMillis()

    //Testing Graph feature extraction
    var testingstartTime = System.currentTimeMillis()

    val result_testing = ShortestPaths.run(testingGraph_gx, testingSamplesSources)

    val shortestPaths_testing = result_testing.vertices.filter(line => line._2.size > 0).
      flatMap({case (k,v) => v.map(vx => (k,vx))}).
      map(line => (line._1, line._2._1, line._2._2)).
      filter(line => line._1 != line._2)

    val shortestPaths_testing_df = shortestPaths_testing.toDF("authorId1", "authorId2","distance")
    shortestPaths_testing_df.persist(StorageLevel.MEMORY_AND_DISK_SER)
    shortestPaths_testing_df.where("distance > 0").show(10);
    println("Count of testing first shortest path records: " + shortestPaths_testing_df.count())

    shortestPaths_testing_df.write.option("sep","|").mode("overwrite").csv(const.dstFolderPath+const.testingfspFolder);

    var testingstopTime = System.currentTimeMillis()

    println("time taken for shortest path extraction for training and testing in seconds are : " + (trainingstopTime-trainingStartTime)/1000 +  " | " + (testingstopTime-testingstartTime)/1000)
  }


  def readFlattenedDF(const: constantsFile): DataFrame = {
    val tmp1: DataFrame = spark.read.option("inferSchema", "true").parquet(const.srcFolderPath + const.flatteneddfFolder + "/*");
    tmp1.createOrReplaceGlobalTempView("t1");
    tmp1.persist()
    println("Count of records: " + tmp1.count())
    return tmp1

  }


  def preCheckCC2(const: constantsFile, cc : DataFrame, samples: DataFrame): DataFrame = {
    cc.createOrReplaceGlobalTempView("c");
    samples.createOrReplaceGlobalTempView("s");

    var tmp1 = spark.sql("SELECT s.*, c.least_vertex_id as lv1 FROM global_temp.c as c inner join global_temp.s as s on c.connected_vertices = s.authorId1")
    var tmp2 = spark.sql("SELECT s.*, c.least_vertex_id as lv2 FROM global_temp.c as c inner join global_temp.s as s on c.connected_vertices = s.authorId2")
    tmp1.createOrReplaceGlobalTempView("t1");
    tmp2.createOrReplaceGlobalTempView("t2");
    tmp1.show(20);
    tmp2.show(20);

    var shortest_path_exists = spark.sql("SELECT t1.authorId1, t1.authorId2, t1.year, t1.expected_label FROM global_temp.t1 as t1 " +
      "inner join global_temp.t2 as t2 on t1.lv1 = t2.lv2 and " +
      "t1.authorId1 = t2.authorId1 and t1.authorId2 = t2.authorId2")
    shortest_path_exists.persist(StorageLevel.DISK_ONLY)
    println("Count of records for whom shortest paths exists: " + shortest_path_exists.count())

    return shortest_path_exists;


  }

}
