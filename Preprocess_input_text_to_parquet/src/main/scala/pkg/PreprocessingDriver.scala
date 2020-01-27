/*
Author: Suraj.B.S
Date: 2020/01/27
Description: Preprocess data into parquet file
 */


package pkg

import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._


object PreprocessingDriver {

  def main(args: Array[String]):Unit = {
    val const = new Constants();
    val spark = SparkSession.builder.master(const.master).appName(const.appName).getOrCreate;

    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._


    def preprocessPaperVertices():Unit = {
      val paper1 = spark.read.schema(const.paperSchema).option("header", "true").option("sep",",").option("inferSchema", "false").csv(const.paper_vertices_input_path);

      var paper2 = paper1.withColumn("file_path",input_file_name().cast(StringType))

      var paper3 = paper2.withColumn("year", substring(col("file_path"), -8, 4).cast(IntegerType))

      val paper4 = paper3.select("paperId","year","paperlabel")
      paper4.show(10);

      println("Count of all paper vertices are: " + paper4.count())
      paper4.select("paperId","year", "paperLabel").write.option("spark.sql.parquet.compression.codec","snappy").mode("overwrite").parquet(const.paper_vertices_output_path)

    }


    def preprocessAuthorVertices():Unit = {
      val author1 = spark.read.schema(const.authorSchema).option("header", "true").option("sep",",").option("inferSchema", "false").csv(const.author_vertices_input_path);
      author1.show(10);
      println("Count of all author vertices are: " + author1.count())
      author1.write.option("spark.sql.parquet.compression.codec","snappy").mode("overwrite").parquet(const.author_vertices_output_path)
    }


    def preprocessAuthorshipEdges():Unit = {
      val authorship1 = spark.read.schema(const.authorshipSchema).option("header", "true").option("sep",",").option("inferSchema", "false").csv(const.authorship_edges_input_path);
      var authorship2 = authorship1.withColumn("authorshipLabel", lit("authorship"));
      authorship2.show(10);
      println("Count of all authorship edges are: " + authorship2.count())
      authorship2.write.option("spark.sql.parquet.compression.codec","snappy").mode("overwrite").parquet(const.authorship_edges_output_path)
    }


    def preprocessAuthorAuthorEdges():Unit = {

      val paperdf = spark.read.option("inferSchema", "true").parquet(const.paper_vertices_output_path + "/*");

      paperdf.show(10);

      val vertexdf = spark.read.option("inferSchema", "true").parquet(const.author_vertices_output_path + "/*");

      vertexdf.show(10);

      val authorshipedgedf = spark.read.option("inferSchema", "true").parquet(const.authorship_edges_output_path + "/*");


      authorshipedgedf.createOrReplaceGlobalTempView("edge_table");
      paperdf.createOrReplaceGlobalTempView("paper_vertex_table")
      authorshipedgedf.show(10);
      vertexdf.show(10);


      // getting 5 million bidirectional combinations for 1 million edges of authorship, for 250 million it might cross 1 billion bidirectional edges,
      //this might blowup the spark cluster, need some smart configuration of resources during join process
      //self join edge table and do a cartesion join on author id to get bi directional edges
      var edgesdf = spark.sql("select t1.authorId as src, t2.authorId as dst, t1.paperId as paperId, " +
        "(case when t3.year IS NULL then 2016 else t3.year end) as year, t1.authorshipLabel as label from global_temp.edge_table as t1 " +
        "inner join global_temp.edge_table as t2 on t1.paperId = t2.paperId and t1.authorId != t2.authorId " +
        "inner join global_temp.paper_vertex_table as t3 on t1.paperId = t3.paperId")

      //edgesdf.persist();
      edgesdf.show(10);

      edgesdf.write.mode("overwrite").parquet(const.author_author_edges_output_path)

      println("--- count of edges is ------ " + edgesdf.count())

    }

    preprocessPaperVertices();
    preprocessAuthorVertices();
    preprocessAuthorshipEdges();
    preprocessAuthorAuthorEdges();

  }

}
