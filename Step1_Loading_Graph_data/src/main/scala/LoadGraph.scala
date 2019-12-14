
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.graphframes._
import org.apache.spark.sql.types._

object LoadGraph extends App{
  override def main(args: Array[String]): Unit =
  {
    val spark = SparkSession.builder.master("local[2]").appName("load_graph_data").config("spark.executor.memory", "3g").getOrCreate();

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

    graph.inDegrees.sort(desc("inDegree")).show(10);

    val results_1989: GraphFrame = graph.filterEdges("year <= 1989").pageRank.resetProbability(0.10).tol(0.01).run();
    results_1989.vertices.toDF("id","label","pagerank").write.csv("/Users/surajshashidhar/Desktop/graphml_10lkh/pagerank_1989");

    val results_1990: GraphFrame = graph.filterEdges("year <= 1990").pageRank.resetProbability(0.15).tol(0.01).run();
    results_1990.vertices.toDF("id","label","pagerank").write.csv("/Users/surajshashidhar/Desktop/graphml_10lkh/pagerank_1990");

    val results_1991: GraphFrame = graph.filterEdges("year <= 1991").pageRank.resetProbability(0.15).tol(0.01).run();
    results_1991.vertices.toDF("id","label","pagerank").write.csv("/Users/surajshashidhar/Desktop/graphml_10lkh/pagerank_1991");
    /*
        var pagerank_Df = results_1989.vertices.toDF("id","label","pagerank").union(results_1990.vertices.toDF("id","label","pagerank")
        .union(results_1991.vertices.toDF("id","label","pagerank")));
        pagerank_Df.write.csv("/Users/surajshashidhar/Desktop/graphml_10lkh/pagerank_final")
        //pagerank_Df.persist()
        //pagerank_Df.show()
        //results.vertices.sort(desc("pagerank")).show(20);
        //results.vertices.write.csv("/Users/surajshashidhar/Desktop/graphml_10lkh/pagerank")

     */
  }

}


