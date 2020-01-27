package pkg

import org.apache.spark.graphx.PartitionStrategy.EdgePartition2D
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.graphframes.GraphFrame
import org.apache.spark.graphx._

class sspUtilities(spark: SparkSession) {

  import spark.implicits._

  def generateSubgraphsFromFile(const: constantsFile): Array[Graph[Row,Row]] =
  {
    val startTime = System.currentTimeMillis()

    val trainingvertexdf = spark.read.option("inferSchema", "true").parquet(const.dstFolderPath + const.trainingsubgraphverticesFolder + "/*");

    val testingvertexdf = spark.read.option("inferSchema", "true").parquet(const.dstFolderPath + const.testingsubgraphverticesFolder + "/*");


    trainingvertexdf.show(10);
    testingvertexdf.show(10);

    val trainingedgesdf = spark.read.option("inferSchema", "true").parquet(const.dstFolderPath + const.trainingsubgraphedgesFolder + "/*");
    val testingedgesdf = spark.read.option("inferSchema", "true").parquet(const.dstFolderPath + const.testingsubgraphedgesFolder + "/*");

    trainingedgesdf.show(10);
    testingedgesdf.show(10);

    //trainingvertexdf.persist(StorageLevel.MEMORY_AND_DISK_SER);
    //trainingedgesdf.persist(StorageLevel.DISK_ONLY);

    //testingvertexdf.persist(StorageLevel.MEMORY_AND_DISK_SER);
    //testingedgesdf.persist(StorageLevel.DISK_ONLY);

    val trainingGraph = GraphFrame(trainingvertexdf, trainingedgesdf);
    val testingGraph = GraphFrame(testingvertexdf, testingedgesdf);

    trainingGraph.inDegrees.show(10);
    testingGraph.inDegrees.show(10);

    val trainingGraph_gx = trainingGraph.toGraphX.partitionBy(EdgePartition2D);
    val testingGraph_gx = testingGraph.toGraphX.partitionBy(EdgePartition2D);

    val stopTime = System.currentTimeMillis();

    println("Time taken to form graph is: " + (stopTime-startTime) )

    return Array(trainingGraph_gx,testingGraph_gx)
  }


  def generateSamplesFromFile(const: constantsFile): Array[Dataset[Row]] = {

    val trainingSamples = spark.read.schema(const.authorTupleSchema).option("sep", "|").option("header", "true").option("inferSchema", "false").csv(const.dstFolderPath+const.trainingsamplesFolder + "/*");
    val testingSamples = spark.read.schema(const.authorTupleSchema).option("sep", "|").option("header", "true").option("inferSchema", "false").csv(const.dstFolderPath+const.testingsamplesFolder + "/*");
    trainingSamples.show(10); testingSamples.show(10);

    return Array(trainingSamples, testingSamples)
  }


  def joinSSPData(const: constantsFile): Unit = {
    val sspsamples = spark.read.schema(const.sspSample).option("inferSchema", "false").option("sep", "|").csv(const.sspsamplePath);
    val sspdata = spark.read.schema(const.sspData).option("inferSchema", "false").option("sep", ",").csv(const.sspdataPath);

    sspdata.show(10);
    sspsamples.show(10);

    sspdata.createOrReplaceGlobalTempView("d");
    sspsamples.createOrReplaceGlobalTempView("s");

    var finalop = spark.sql("SELECT s.authorId1, s.authorId2, COALESCE(d.first_shortest_path, 0.0) as fsp, COALESCE(d.second_shortest_path, 0.0) as ssp FROM global_temp.s as s left outer join global_temp.d as d on s.authorId1 = d.authorId1 and s.authorId2 = d.authorId2 ")

    finalop.show(10);
    print(finalop.count())

    finalop.write.option("sep","|").csv("file:///Users/surajshashidhar/Desktop/dataset_20200127/ssp_testing_output");

  }

}
