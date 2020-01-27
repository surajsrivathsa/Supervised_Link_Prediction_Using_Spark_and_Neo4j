package pkg

import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.graphframes.GraphFrame

class pagerankUtilities(spark: SparkSession) {

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


  def createCompleteGraphFromFile(const: constantsFile): GraphFrame =
  {
    val startTime = System.currentTimeMillis()

    val vertexdf = spark.read.schema(const.vertexSchema).option("inferSchema", "false").parquet(const.srcFolderPath + const.subgraphvertexFolder + "/*");

    vertexdf.show(10);

    val edgesdf = spark.read.schema(const.edgesSchema).option("inferSchema", "false").parquet(const.srcFolderPath + const.subgraphedgesFolder + "/*");

    edgesdf.show(10);
    vertexdf.persist(StorageLevel.MEMORY_AND_DISK_SER);
    edgesdf.persist(StorageLevel.DISK_ONLY);

    val graph = GraphFrame(vertexdf, edgesdf);

    graph.inDegrees.show(10);
    val stopTime = System.currentTimeMillis();

    println("Time taken to form graph is: " + (stopTime-startTime) )

    println("Count of vertices and edges in the graph are: " + graph.vertices.count() + " | " + graph.edges.count())

    return graph
  }


  def calc_StaticPageRank(trainingGraph: GraphFrame, testingGraph: GraphFrame, const:constantsFile): Unit =
  {


    val trainingstarttime = System.currentTimeMillis()

    val results_graphx_year1 = trainingGraph.toGraphX
    results_graphx_year1.persist();

    println("------- Persisted graph of year1 in pagerank, count of the vertices is --------------- " + results_graphx_year1.vertices.count())

    val pagerank_year1 = results_graphx_year1.staticPageRank(const.pagerank_iteration,const.pagerank_probability)
    pagerank_year1.persist();
    pagerank_year1.vertices.map(row => row._1.asInstanceOf[Long] + "," + row._2).saveAsTextFile(const.dstFolderPath+const.trainingpagerankFolder);


    val trainingendtime = System.currentTimeMillis()


    val testingstarttime = System.currentTimeMillis()

    val results_graphx_year2 = testingGraph.toGraphX
    results_graphx_year2.persist();

    println("-------- Persisted graph of year2 in pagerank, count of the vertices is ----------- "  + results_graphx_year2.vertices.count())

    val pagerank_year2 = results_graphx_year2.staticPageRank(const.pagerank_iteration,const.pagerank_probability)
    pagerank_year2.persist();
    pagerank_year2.vertices.map(row => row._1.asInstanceOf[Long] + "," + row._2).saveAsTextFile(const.dstFolderPath+const.testingpagerankFolder);

    val testingendtime = System.currentTimeMillis()

    println("Time taken to run pagerank for training and testing graphs in seconds are: " + (trainingendtime-trainingstarttime)/1000 + " | " + (testingendtime-testingstarttime)/1000)

  }


  def calc_DynamicPageRank(trainingGraph: GraphFrame, testingGraph: GraphFrame, const:constantsFile): Unit =
   {
      val trainingstarttime = System.currentTimeMillis()
      var trainingDynamicPagerank = trainingGraph.pageRank.resetProbability(0.15).tol(0.01).run()
      trainingDynamicPagerank.vertices.select("id","pagerank").show(10)
      trainingDynamicPagerank.vertices.select("id","pagerank").write.option("sep","|").mode("overwrite").csv(const.dstFolderPath+const.trainingdynamicpagerankFolder)

      var trainingstoptime = System.currentTimeMillis()

      val testingstarttime = System.currentTimeMillis()
      var testingDynamicPagerank = testingGraph.pageRank.resetProbability(0.15).tol(0.01).run()
      testingDynamicPagerank.vertices.select("id","pagerank").show(10)
      testingDynamicPagerank.vertices.select("id","pagerank").write.option("sep","|").mode("overwrite").csv(const.dstFolderPath+const.testingdynamicpagerankFolder)

      var testingstoptime = System.currentTimeMillis()

      println("Time taken for calculating dynammic pagerank for training and testing in seconds are: " + (trainingstoptime-trainingstarttime)/1000 + " | " + (testingstoptime-testingstarttime)/1000)



    }


  def calcDynamicGlobalPageRank(graph: GraphFrame, const: constantsFile): Unit = {

    val starttime = System.currentTimeMillis()
    var globalDynamicPagerank = graph.pageRank.resetProbability(0.15).tol(0.01).run()
    println(" -----  global page rank --------- ");
    globalDynamicPagerank.vertices.show(10);
    globalDynamicPagerank.vertices.select("id","pagerank").write.option("sep","|").mode("overwrite").csv(const.dstFolderPath+const.trainingglobalpagerankFolder)
    globalDynamicPagerank.vertices.select("id","pagerank").write.option("sep","|").mode("overwrite").csv(const.dstFolderPath+const.testingglobalpagerankFolder)


    var stoptime = System.currentTimeMillis()

    println("Time taken for calculating global pagerank for training and testing in seconds are: " + (stoptime-starttime)/1000 )

    println("count of vertices for pagerank is: " + globalDynamicPagerank.vertices.count())


  }





}
