package pkg

import org.apache.spark.graphx.EdgeDirection
import org.apache.spark.graphx.PartitionStrategy.EdgePartition2D
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.graphframes.GraphFrame

class commonneighboursUtilities(spark: SparkSession) {


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

/*
First collect distinct  neighbours of each vertex
Join them with the samples example (1, [2,3,4,5], 8, [5,3,10,12])
Use intersection algorithm on two sets and find the size of the intersected set this gives us common neighbours
 */
  def generateCommonNeighbours(trainingGraph: GraphFrame, testingGraph: GraphFrame, trainingSamples: Dataset[Row],testingSamples: Dataset[Row], const: constantsFile): Unit =
  {

    //Define user deifined functions that can do intersection and calculate length of common neighbour arrays, these will be used later

    val intersection = udf((x: Seq[Long], y: Seq[Long]) => x.distinct.intersect(y.distinct))
    spark.udf.register("intersection", intersection);

    val lenx = udf((x: Seq[Long], y: Seq[Long]) => x.distinct.intersect(y.distinct).length)
    spark.udf.register("lenx", lenx);

    var trainingStart = System.currentTimeMillis()

    //converte the graphs from graphframe to graphx format and use edgepartition2d strateguy so that most of the edges having two vertices end up in same partition

    val gx_year1 = trainingGraph.toGraphX;
    trainingSamples.createOrReplaceGlobalTempView("tr");
    gx_year1.partitionBy(EdgePartition2D)

    //collect the neighbours of each node using inbuit api
    val neighbours_year1 = gx_year1.collectNeighborIds(EdgeDirection.Out);

    //filter out any vertices that doesn't have any neighbours
    val op1 = neighbours_year1.filter(line => line._2.length > 0).map(row => (row._1, row._2))
    val df_op1 = op1.toDF()
    df_op1.createOrReplaceGlobalTempView("df_op1");
    df_op1.persist();
    df_op1.show(10)

    //join the author tuples with the neighbours
    //example output(authorId1, neighbour1, authorId2, neighbour2) => (1234, [34, 25, 256], 789, [256, 25, 89])
    var interim1 = spark.sql("SELECT t1.authorId1, t2._2 as neigbours1, t1.authorId2 FROM global_temp.tr as t1 LEFT OUTER JOIN global_temp.df_op1 as t2 " +
      "ON t1.authorId1 = t2._1")


    println("interim1 count: " + interim1.count())
    interim1.createOrReplaceGlobalTempView("i1");

    var interim2 = spark.sql("SELECT DISTINCT t1.authorId1 as authorId1, t1.neigbours1 as neighbours1, t1.authorId2 as authorId2, t2._2 as neighbours2 FROM global_temp.i1 as t1 LEFT OUTER JOIN global_temp.df_op1 as t2 " +
      "ON t1.authorId2 = t2._1")

    interim2.createOrReplaceGlobalTempView("i2");
    interim2.persist()
    interim2.show(10);
    println("interim2 count: " + interim2.count())
    //apply intersection and length udf to the above dataframe to get length of common neighbours
    //(1234, [34, 25, 256], 789, [256, 25, 89]) => (1234, 789, [256, 25]) => (1234, 789, 2)

    spark.sql("SELECT t.authorId1, t.authorId2, lenx(t.neighbours1, t.neighbours2) as common_neighbours,intersection(t.neighbours1, t.neighbours2) as cn_array FROM global_temp.i2 as t where t.neighbours1 is not null and t.neighbours2 is not null").show(100)
    val cn_exists_training = spark.sql("SELECT t.authorId1, t.authorId2, lenx(t.neighbours1, t.neighbours2) as common_neighbours FROM global_temp.i2 as t where t.neighbours1 is not null and t.neighbours2 is not null").repartition(1);
    val cn_notexists_training = spark.sql("SELECT t.authorId1, t.authorId2, 0 as common_neighbours FROM global_temp.i2 as t where t.neighbours1 is  null or t.neighbours2 is null")
    val all_cn_training = cn_notexists_training.union(cn_exists_training)

    //save the output to file
    all_cn_training.write.option("sep", "|").mode("overwrite").csv(const.dstFolderPath+const.trainingcnFolder )


    var trainingEnd= System.currentTimeMillis()


    //Do the same above steps for testing data
    var testingStart = System.currentTimeMillis()

    val gx_year2 = testingGraph.toGraphX;
    testingSamples.createOrReplaceGlobalTempView("te");
    gx_year2.partitionBy(EdgePartition2D)

    val neighbours_year2 = gx_year2.collectNeighborIds(EdgeDirection.Out);

    var training_StartTime = System.currentTimeMillis()


    var op2 = neighbours_year2.filter(line => line._2.length > 0).map(row => (row._1, row._2));
    var df_op2 = op2.toDF();
    df_op2.createOrReplaceGlobalTempView("df_op2");
    df_op2.persist();
    df_op2.show(10)
    println(" count of records with neigbours : " + df_op2.count());


    var interim3 = spark.sql("SELECT t1.authorId1, t2._2 as neigbours1, t1.authorId2 FROM global_temp.te as t1 LEFT OUTER JOIN global_temp.df_op2 as t2 " +
      "ON t1.authorId1 = t2._1")

    interim3.createOrReplaceGlobalTempView("i3");
    println("interim3 count: " + interim3.count())

    var interim4 = spark.sql("SELECT DISTINCT t1.authorId1 as authorId1, t1.neigbours1 as neighbours1, t1.authorId2 as authorId2, t2._2 as neighbours2 FROM global_temp.i3 as t1 LEFT OUTER JOIN global_temp.df_op2 as t2 " +
      "ON t1.authorId2 = t2._1")

    interim4.createOrReplaceGlobalTempView("i4");
    interim4.persist()
    interim4.show(10);
    println("interim4 count: " + interim4.count())

    spark.sql("SELECT t.authorId1, t.authorId2, lenx(t.neighbours1, t.neighbours2) as common_neighbours,intersection(t.neighbours1, t.neighbours2) as cn_array FROM global_temp.i4 as t where t.neighbours1 is not null and t.neighbours2 is not null").show(100)
    val cn_exists_testing = spark.sql("SELECT t.authorId1, t.authorId2, lenx(t.neighbours1, t.neighbours2) as common_neighbours FROM global_temp.i4 as t where t.neighbours1 is not null and t.neighbours2 is not null").repartition(1);
    val cn_notexists_testing = spark.sql("SELECT t.authorId1, t.authorId2, 0 as common_neighbours FROM global_temp.i4 as t where t.neighbours1 is  null or t.neighbours2 is null")
    val all_cn_testing = cn_notexists_testing.union(cn_exists_testing)

    all_cn_testing.write.option("sep", "|").mode("overwrite").csv(const.dstFolderPath+const.testingcnFolder )

    var testingEnd = System.currentTimeMillis()

    println("-------   Count of edges in subgraphs are " + gx_year1.edges.count() + " , " + gx_year2.edges.count() + "   ----------  ");
    println("- count of training and testing data is: " + all_cn_training.count + " | " + all_cn_testing.count())
    println("Time taken to create training and testing common neighbours dataset in seconds are: " + (trainingEnd-trainingStart)/1000 + " | " + (testingEnd-testingStart)/1000 )

  }


}
