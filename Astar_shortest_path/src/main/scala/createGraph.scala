
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.graphx._
import org.apache.spark.graphx.lib.ShortestPaths
import org.apache.spark.sql.{SparkSession, _}
import org.apache.spark.sql.functions._
import graphframes._
import org.graphframes.GraphFrame
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD



class createGraph(spark: SparkSession) {
  import spark.implicits._

  def createGraphFromFile(): GraphFrame = {

    val paperSchema = StructType(Array(StructField("paperId", LongType, nullable = false), StructField("year", IntegerType, nullable = false), StructField("paperLabel", StringType, nullable = false)))

    val authorSchema = StructType(Array(StructField("id", LongType, nullable = false), StructField("label", StringType, nullable = false)))

    val authorshipSchema = StructType(Array(StructField("paperId", LongType, nullable = false), StructField("authorId", LongType, nullable = false), StructField("Label", StringType, nullable = false)))

    val paperdf = spark.read.schema(paperSchema).option("header", "false").option("inferSchema", "false").csv("/Users/surajshashidhar/Desktop/graphml_10lkh/Papers_10lkh.csv");

    val vertexdf = spark.read.schema(authorSchema).option("header", "false").option("inferSchema", "false").csv("/Users/surajshashidhar/Desktop/graphml_10lkh/Authors_10lkh.csv");

    val authorshipedgedf = spark.read.schema(authorshipSchema).option("header", "false").option("inferSchema", "false").csv("/Users/surajshashidhar/Desktop/graphml_10lkh/Authorship_10lkh.csv");

    authorshipedgedf.createOrReplaceGlobalTempView("edge_table");
    paperdf.createOrReplaceGlobalTempView("paper_vertex_table")
    authorshipedgedf.show(10);

    /*
    var edgesdf = spark.sql("select t1.authorId as src, t2.authorId as dst, t1.paperId as paperId, t3.year as year, t1.Label as label from global_temp.edge_table as t1 " +
      "inner join global_temp.edge_table as t2 on t1.paperId = t2.paperId and t1.authorId != t2.authorId " +
      "inner join global_temp.paper_vertex_table as t3 on t1.paperId = t3.paperId")
     */

    var edgesdf2 = spark.sql("select t1.authorId as src, t2.authorId as dst, t1.paperId as paperId, t1.Label as label from global_temp.edge_table as t1 " +
      "inner join global_temp.edge_table as t2 on t1.paperId = t2.paperId and t1.authorId != t2.authorId ")

    edgesdf2.createOrReplaceGlobalTempView("edgesdf2");

    var edgesdf3 = spark.sql("select t1.src, t1.dst, t1.paperId, t2.year as year, t1.Label as label from global_temp.edgesdf2 as t1 " +
      "inner join global_temp.paper_vertex_table as t2 on t1.paperId = t2.paperId")


    //edgesdf.coalesce(1).write.option("header","true").option("sep","|").mode("overwrite").csv("/Users/surajshashidhar/Desktop/graphml_10lkh/edgesdf1")///

    edgesdf3.coalesce(1).write.option("header","true").option("sep","|").mode("overwrite").csv("/Users/surajshashidhar/Desktop/graphml_10lkh/edgesdf3");


    edgesdf2.persist();
    edgesdf2.show(10);

    vertexdf.persist();
    vertexdf.show(10);

    val graph = GraphFrame(vertexdf, edgesdf2);

    graph.triplets.take(10)

    return graph;
  }

  def uat(): RDD[Row] = {
    val mlschema = StructType(Array(StructField("authorId1", LongType, nullable = false), StructField("authorId2", LongType, nullable = false), StructField("label", StringType, nullable = false),StructField("common_neighbours", DoubleType, nullable = false),StructField("pagerankscore1", DoubleType, nullable = false), StructField("pagerankscore2", DoubleType, nullable = false), StructField("preferential_attachment_score1", DoubleType, nullable = false), StructField("preferential_attachment_score2", DoubleType, nullable = false)))
    var mldf = spark.read.schema(mlschema).format("csv").option("header", "false").option("mode", "DROPMALFORMED").option("inferSchema", "false").load("/Users/surajshashidhar/Desktop/graphml_10lkh/featureset4/featureset4.csv");
    mldf.show(10);
    var rt = mldf.select("authorId1", "authorId2").limit(100).rdd
    return rt

  }

}
