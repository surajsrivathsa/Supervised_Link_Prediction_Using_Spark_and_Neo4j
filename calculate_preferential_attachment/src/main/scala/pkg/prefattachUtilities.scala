package pkg

import java.math.BigDecimal

import org.apache.spark.graphx.EdgeDirection
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.graphframes.GraphFrame

class prefattachUtilities(spark: SparkSession) {


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

  def preferentialAttachment(trainingGraph: GraphFrame, testingGraph: GraphFrame ,const: constantsFile) : Unit =
  {

    var year1 = const.year1; var year2 = const.year2; var year3 = const.year3;
    println("------ " + year1 + " : " + year2 + " : " + year3 + " ------------")

    //val graph_year3 = graph.filterEdges(const.year3_filter);
    val gx_year1 = trainingGraph.toGraphX;
    val gx_year2 = testingGraph.toGraphX;
    //val gx_year3 = graph_year3.toGraphX;

    gx_year1.persist(); gx_year2.persist(); //gx_year3.persist()

    //normalize the length nu 2* number of edges as its undirected
    val trainingstarttime = System.currentTimeMillis()
    val neighbours_year1 = gx_year1.collectNeighborIds(EdgeDirection.Either);
    val totalEdges_year1 = gx_year1.edges.count();
    var pref_attachment_year1 = neighbours_year1.map(line => (line._2.length.toDouble/(2.0*totalEdges_year1),line._2.length,line._1));
    val trainingendtime = System.currentTimeMillis()


    val testingstarttime = System.currentTimeMillis()
    val neighbours_year2 = gx_year2.collectNeighborIds(EdgeDirection.Either);
    val totalEdges_year2 = gx_year2.edges.count();
    var pref_attachment_year2 = neighbours_year2.map(line => (line._2.length.toDouble/(2.0*totalEdges_year2),line._2.length,line._1));
    val testingendtime = System.currentTimeMillis()

    println("Time taken to calculate preferential attachment for training and testing graphs in seconds are: " + (trainingendtime-trainingstarttime)/1000 + " | " + (testingendtime-testingstarttime)/1000)
    println("   -----------------------  "+ "the count of edges for the three years are: " + totalEdges_year1 + " ---- " + totalEdges_year2 );

    pref_attachment_year1.map(line => new BigDecimal(line._1).toPlainString() + "|" + line._2 + "|" + line._3 + "|" + year1).saveAsTextFile(const.dstFolderPath + const.trainingprefattachFolder);
    pref_attachment_year2.map(line => new BigDecimal(line._1).toPlainString() + "|" + line._2 + "|" + line._3 + "|" + year2).saveAsTextFile(const.dstFolderPath + const.testingprefattachFolder);

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


  def globalpreferentialAttachment(graph: GraphFrame ,const: constantsFile) : Unit =
  {

    var year1 = const.year1; var year2 = const.year2; var year3 = const.year3;
    println("------ " + year1 + " : " + year2 + " : " + year3 + " ------------")

    //val graph_year3 = graph.filterEdges(const.year3_filter);
    val gx_year1 = graph.toGraphX;

    gx_year1.persist()


    val trainingstarttime = System.currentTimeMillis()
    val neighbours_year1 = gx_year1.collectNeighborIds(EdgeDirection.Out);
    val totalEdges_year1 = gx_year1.edges.count();
    var pref_attachment_year1 = neighbours_year1.map(line => (line._2.length.toDouble/(2.0*totalEdges_year1),line._2.length,line._1));
    val trainingendtime = System.currentTimeMillis()

    println("---- count of global edges is: " + totalEdges_year1)


    pref_attachment_year1.map(line => new BigDecimal(line._1).toPlainString() + "|" + line._2 + "|" + line._3 + "|" + year1).saveAsTextFile(const.dstFolderPath + const.trainingglobalprefattachFolder);
    pref_attachment_year1.map(line => new BigDecimal(line._1).toPlainString() + "|" + line._2 + "|" + line._3 + "|" + year1).saveAsTextFile(const.dstFolderPath + const.testingglobalprefattachFolder);

  }


}
