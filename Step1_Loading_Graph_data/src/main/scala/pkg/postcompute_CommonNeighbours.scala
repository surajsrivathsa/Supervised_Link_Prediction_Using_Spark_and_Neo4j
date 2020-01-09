package pkg

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.graphframes.GraphFrame

class postcompute_CommonNeighbours (spark: SparkSession){

  import spark.implicits._
  def generateCommonNeighbours( graph: GraphFrame, const: constantsFile): Unit =
  {
    val graph_year1 = graph.filterEdges(const.year1_filter);
    val graph_year2 = graph.filterEdges(const.year2_filter);
    //val graph_year3 = graph.filterEdges(const.year3_filter);
    graph_year1.persist(); graph_year2.persist();
    //graph_year3.persist()

    println("---- count of edges in graphs of year 1,2,3 are : " + graph_year1.edges.count() + " : " +  graph_year2.edges.count() + " : " +  -1);


    val commonneighbours_path = (1 to 2).map(i => s"(n$i)-[e$i]->(n${i + 1})").mkString(";")
    println(commonneighbours_path);
    val commonneighbours_filter = (1 to 3).map(i => s"n$i").combinations(2).map { case Seq(i, j) => col(i) !== col(j) }.reduce(_ && _)
    print(commonneighbours_filter);

    println(" ---------- the filters and paths are: " + commonneighbours_path + "    ------ " + commonneighbours_filter.toString());

    var sdf_year1 = graph_year1.find(commonneighbours_path).filter(commonneighbours_filter).select(col("n1.id").alias("authorId1"),col("n3.id").alias("authorId2"));
    var sdf_year2 = graph_year2.find(commonneighbours_path).filter(commonneighbours_filter).select(col("n1.id").alias("authorId1"),col("n3.id").alias("authorId2"));
    //var sdf_year3 = graph_year3.find(commonneighbours_path).filter(commonneighbours_filter).select(col("n1.id").alias("authorId1"),col("n3.id").alias("authorId2"));




    /*
    //sdf_year1.persist(); sdf_year2.persist(); sdf_year3.persist();

    //println( "SDF count for yers 1,2 and 3 are ---------  " + sdf_year1.count() + " : " + sdf_year2.count() + " : " + sdf_year3.count() );

    var tdf_year1 = sdf_year1.select(col("n1.id").alias("authorId1"),col("n3.id").alias("authorId2"));
    var tdf_year2 = sdf_year2.select(col("n1.id").alias("authorId1"),col("n3.id").alias("authorId2"));
    var tdf_year3 = sdf_year3.select(col("n1.id").alias("authorId1"),col("n3.id").alias("authorId2"));

    tdf_year1.show(10);
    tdf_year2.show(10);
    tdf_year3.show(10);

    tdf_year1.createOrReplaceGlobalTempView("t1");
    tdf_year2.createOrReplaceGlobalTempView("t2");
    tdf_year3.createOrReplaceGlobalTempView("t3");

    var res_year1 = spark.sql("select t1.authorId1 as authorId1, t1.authorId2  as authorId2 from global_temp.t1 as t1 ");
    var res_year2 = spark.sql("select t2.authorId1 as authorId1, t2.authorId2  as authorId2 from global_temp.t2 as t2 ");
    var res_year3 = spark.sql("select t3.authorId1 as authorId1, t3.authorId2  as authorId2 from global_temp.t3 as t3 ");

    res_year1.show(10);
    res_year2.show(10);
    res_year3.show(10);

    sdf_year1.show(10);
    sdf_year2.show(10);
    sdf_year3.show(10);
*/

    sdf_year1.write.option("spark.sql.parquet.compression.codec", "snappy").mode("overwrite").parquet(const.dstFolderPath+const.commonneighbourstmpFileName+const.year1)
    sdf_year2.write.option("spark.sql.parquet.compression.codec", "snappy").mode("overwrite").parquet(const.dstFolderPath+const.commonneighbourstmpFileName+const.year2)
    //sdf_year3.write.option("spark.sql.parquet.compression.codec", "snappy").mode("overwrite").parquet(const.dstFolderPath+const.commonneighbourstmpFileName+const.year3)



    /*
    sdf_year1.write.option("sep", "|").mode("overwrite").csv(const.dstFolderPath+const.commonneighbourstmpFileName+const.year1);
    sdf_year2.write.option("sep", "|").mode("overwrite").csv(const.dstFolderPath+const.commonneighbourstmpFileName+const.year1);
    sdf_year3.write.option("sep", "|").mode("overwrite").csv(const.dstFolderPath+const.commonneighbourstmpFileName+const.year1);

     */


  }

}
