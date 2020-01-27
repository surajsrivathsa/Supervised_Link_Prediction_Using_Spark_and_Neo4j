package pkg

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.desc
import org.graphframes.GraphFrame

class preprocessingUtilities(spark: SparkSession) {

  def createCompleteGraph(const: constantsFile): Unit =
  {
    //Read paper,author and authorship files
    val paperdf = spark.read.schema(const.paperSchema).option("inferSchema", "false").csv(const.srcFolderPath + const.paperFileName);

    paperdf.show(10);

    val authordf = spark.read.schema(const.authorSchema).option("inferSchema", "false").csv(const.srcFolderPath + const.authorFileName);

    val vertexdf = authordf.withColumnRenamed("authorId", "id").withColumnRenamed("authorLabel", "label")

    vertexdf.show(10);

    val authorshipedgedf = spark.read.schema(const.authorshipSchema).option("inferSchema", "false").csv(const.srcFolderPath + const.authorshipFileName);

    authorshipedgedf.show(10);

    authorshipedgedf.createOrReplaceGlobalTempView("edge_table");
    paperdf.createOrReplaceGlobalTempView("paper_vertex_table")

    /*
    We have paper to author edges, Our aim in this SQL is to reduce this to edges between authors.
    Hence the paper vertex properties such as year/paperid will be now edge property.
     */
    // getting 5 million bidirectional combinations for 1 million edges of authorship, for 250 million it might cross 1 billion bidirectional edges,
    //this might blowup the spark cluster, need some smart configuration of resources during join process
    var edgesdf = spark.sql("select t1.authorId as src, t2.authorId as dst, t1.paperId as paperId, " +
      "(case when t3.year IS NULL then 2016 else t3.year end) as year, t1.authorshipLabel as label from global_temp.edge_table as t1 " +
      "inner join global_temp.edge_table as t2 on t1.paperId = t2.paperId and t1.authorId != t2.authorId " +
      "inner join global_temp.paper_vertex_table as t3 on t1.paperId = t3.paperId")

    //edgesdf.persist();
    edgesdf.show(10);

    //write edges file to directory using snappy compressed parquet as this takes less space and read speed
    edgesdf.write.option("spark.sql.parquet.compression.codec","snappy").mode("overwrite").parquet(const.srcFolderPath + const.edgeFolderName)

    println("--- count of edges is ------ " + edgesdf.count())

    //vertexdf.persist();
    vertexdf.show(10);

    //write vertex file to directory using snappy compressed parquet as this takes less space and read speed
    vertexdf.write.option("spark.sql.parquet.compression.codec","snappy").mode("overwrite").parquet(const.srcFolderPath + const.vertexFolderName)

    println("--- count of vertices is ------ " + vertexdf.count())

    val graph = GraphFrame(vertexdf, edgesdf);

    //test to check whether graph can be created properly using vertex and edge file
    graph.inDegrees.sort(desc("inDegree")).show(10);

    return ;
  }


}
