
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.graphframes._;
import org.apache.spark.sql.types._


object LoadGraph extends App{
  override def main(args: Array[String]): Unit =
  {
    val const = new constantsFile();
    val spark = SparkSession.builder.master(const.master).appName(const.appName).getOrCreate();

    //Setting loglevel to warning as we don't need info
    //.config("spark.executor.memory", const.executor_memory).config("spark.driver.memory", const.driver_memory)
    spark.sparkContext.setLogLevel("WARN")


    val t1 = System.currentTimeMillis
    val paperdf = spark.read.schema(const.paperSchema).option("header", "false").option("inferSchema", "false").csv(const.srcFolderPath + const.paperFileName);

    val vertexdf = spark.read.schema(const.authorSchema).option("header", "false").option("inferSchema", "false").csv(const.srcFolderPath + const.authorFileName);

    val authorshipedgedf = spark.read.schema(const.authorshipSchema).option("header", "false").option("inferSchema", "false").csv(const.srcFolderPath + const.authorshipFileName);

    authorshipedgedf.createOrReplaceGlobalTempView("edge_table");
    paperdf.createOrReplaceGlobalTempView("paper_vertex_table")
    authorshipedgedf.show(10);
    vertexdf.show(10);


    // getting 5 million bidirectional combinations for 1 million edges of authorship, for 250 million it might cross 1 billion bidirectional edges,
    //this might blowup the spark cluster, need some smart configuration of resources during join process
    var edgesdf = spark.sql("select t1.authorId as src, t2.authorId as dst, t1.paperId as paperId, " +
      "(case when t3.year IS NULL then 1989 else t3.year end) as year, t1.Label as label from global_temp.edge_table as t1 " +
      "inner join global_temp.edge_table as t2 on t1.paperId = t2.paperId and t1.authorId != t2.authorId " +
      "left join global_temp.paper_vertex_table as t3 on t1.paperId = t3.paperId")

    edgesdf.persist();
    edgesdf.show(10);

    println("--- count of edges is ------ " + edgesdf.count())

    vertexdf.persist();
    vertexdf.show(10);

    println("--- count of vertices is ------ " + vertexdf.count())

    val graph = GraphFrame(vertexdf, edgesdf);

    graph.inDegrees.sort(desc("inDegree")).show(10);

    val t2 = System.currentTimeMillis

    println(" ---------  It took " + ((t2-t1)) + " milliseconds for graph loading and creation -----------")

    /*
    val t3 = System.currentTimeMillis
    val pc_pagerank = new precompute_Pagerank(spark)
    pc_pagerank.calc_PageRank(graph,const)
    val t4 = System.currentTimeMillis

    println(" ---------  It took " + ((t4-t3)) + " milliseconds for pagerank computation and writing to a file -----------")

    val t5 = System.currentTimeMillis
    val pa = new precompute_PreferentialAttachment(spark)
    pa.preferentialAttachment(graph,const);
    val t6 = System.currentTimeMillis

    println(" ---------  It took " + ((t6-t5)) + " milliseconds for preferntial attachment computation and writing to a file -----------")
     */

    val t7 = System.currentTimeMillis

    val s2le = new Step2_Linkexistence(spark);
    //s2le.generateLinkExistsSamples(graph,const);
    s2le.generateSamples(graph, const);

    val t8 = System.currentTimeMillis
    println(" ---------  It took " + ((t8-t7)) + " milliseconds for step2 sample creation -----------")

    //val step3start = new assembleFeatures(spark);
    //step3start.featureAssembling;

    //val mll = new machinelearning(spark);
    //mll.ml;

    /*
    val paperSchema = StructType(Array(StructField("paperId", LongType, nullable = false), StructField("year", IntegerType, nullable = false) , StructField("paperLabel", StringType, nullable = false)))

    val authorSchema = StructType(Array(StructField("id", LongType, nullable = false),StructField("label", StringType, nullable = false)))

    val authorshipSchema = StructType(Array(StructField("paperId", LongType, nullable = false), StructField("authorId", LongType, nullable = false), StructField("Label", StringType, nullable = false)))

    val paperdf = spark.read.schema(paperSchema).option("header", "false").option("inferSchema", "false").csv("/Users/surajshashidhar/Desktop/graphml_10lkh/Papers_10lkh.csv");

    val vertexdf = spark.read.schema(authorSchema).option("header", "false").option("inferSchema", "false").csv("/Users/surajshashidhar/Desktop/graphml_10lkh/Authors_10lkh.csv");

    val authorshipedgedf = spark.read.schema(authorshipSchema).option("header", "false").option("inferSchema", "false").csv("/Users/surajshashidhar/Desktop/graphml_10lkh/Authorship_10lkh.csv");

    authorshipedgedf.createOrReplaceGlobalTempView("edge_table");
    paperdf.createOrReplaceGlobalTempView("paper_vertex_table")
    authorshipedgedf.show(10);

    var edgesdf = spark.sql("select t1.authorId as src, t2.authorId as dst, t1.paperId as paperId, t3.year as year, t1.Label as label from global_temp.edge_table as t1 " +
      "inner join global_temp.edge_table as t2 on t1.paperId = t2.paperId and t1.authorId != t2.authorId " +
      "inner join global_temp.paper_vertex_table as t3 on t1.paperId = t3.paperId")

    edgesdf.persist();
    edgesdf.show(10);

    vertexdf.persist();
    vertexdf.show(10);

    val graph = GraphFrame(vertexdf, edgesdf);

    //graph.vertices.toDF("id")
    graph.inDegrees.sort(desc("inDegree")).show(10);


    //val step2start = new Step2_Linkexistence(spark);
    //step2start.myfunction(graph);
    //val pa = new computePreferentialattachment(spark);
    //pa.preferentialAttachment(graph);
*/

    /*
    val results_1989: GraphFrame = graph.filterEdges("year <= 1989").pageRank.resetProbability(0.10).tol(0.01).run();
    results_1989.vertices.toDF("id","label","pagerank").write.csv("/Users/surajshashidhar/Desktop/graphml_10lkh/pagerank_1989");

    val results_1990: GraphFrame = graph.filterEdges("year <= 1990").pageRank.resetProbability(0.15).tol(0.01).run();
    results_1990.vertices.toDF("id","label","pagerank").write.csv("/Users/surajshashidhar/Desktop/graphml_10lkh/pagerank_1990");

    val results_1991: GraphFrame = graph.filterEdges("year <= 1991").pageRank.resetProbability(0.15).tol(0.01).run();
    results_1991.vertices.toDF("id","label","pagerank").write.csv("/Users/surajshashidhar/Desktop/graphml_10lkh/pagerank_1991");

     */

    /*
        var pagerank_Df = results_1989.vertices.toDF("id","label","pagerank").union(results_1990.vertices.toDF("id","label","pagerank")
        .union(results_1991.vertices.toDF("id","label","pagerank")));
        pagerank_Df.write.csv("/Users/surajshashidhar/Desktop/graphml_10lkh/pagerank_final")
        //pagerank_Df.persist()
        //pagerank_Df.show()
        //results.vertices.sort(desc("pagerank")).show(20);
        //results.vertices.write.csv("/Users/surajshashidhar/Desktop/graphml_10lkh/pagerank")

https://stackoverflow.com/questions/41873291/write-spark-dataframe-to-file-using-python-and-delimiter
     */

  }

}


