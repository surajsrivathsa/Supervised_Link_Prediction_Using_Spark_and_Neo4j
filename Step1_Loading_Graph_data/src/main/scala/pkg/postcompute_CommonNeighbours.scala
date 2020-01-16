package pkg

import org.apache.spark.graphx.EdgeDirection
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.graphframes.GraphFrame
import org.apache.spark.sql.functions.udf



class postcompute_CommonNeighbours (spark: SparkSession){

  import spark.implicits._

  def generateCommonNeighbours3( trainingGraph: GraphFrame, testingGraph: GraphFrame, const: constantsFile): Unit =
  {

    println("---- count of edges in graphs of year 1,2,3 are : " + trainingGraph.edges.count() + " : " +  testingGraph.edges.count() + " : " +  -1);


    val commonneighbours_path = (1 to 2).map(i => s"(n$i)-[e$i]->(n${i + 1})").mkString(";")
    println(commonneighbours_path);
    val commonneighbours_filter = (1 to 3).map(i => s"n$i").combinations(2).map { case Seq(i, j) => col(i) !== col(j) }.reduce(_ && _)
    print(commonneighbours_filter);

    println(" ---------- the filters and paths are: " + commonneighbours_path + "    ------ " + commonneighbours_filter.toString());

    var sdf_year1 = trainingGraph.find(commonneighbours_path).filter(commonneighbours_filter).select(col("n1.id").alias("authorId1"),col("n3.id").alias("authorId2"));
    var sdf_year2 = testingGraph.find(commonneighbours_path).filter(commonneighbours_filter).select(col("n1.id").alias("authorId1"),col("n3.id").alias("authorId2"));
    //var sdf_year3 = graph_year3.find(commonneighbours_path).filter(commonneighbours_filter).select(col("n1.id").alias("authorId1"),col("n3.id").alias("authorId2"));

    println(" The count of records in training and testing data is : " + sdf_year1.count() + " -- " + sdf_year2.count())
    sdf_year1.createOrReplaceGlobalTempView("t")
    spark.sql("SELECT t.authorId1, t.authorId2, count(*) as commonneighbour FROM global_temp.t group by t.authorId1, t.authorId2 order by commonneighbour desc").show(100)

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

    //sdf_year1.write.option("spark.sql.parquet.compression.codec", "snappy").mode("overwrite").parquet(const.dstFolderPath+const.commonneighbourstmpFileName+const.year1)
    //sdf_year2.write.option("spark.sql.parquet.compression.codec", "snappy").mode("overwrite").parquet(const.dstFolderPath+const.commonneighbourstmpFileName+const.year2)
    //sdf_year3.write.option("spark.sql.parquet.compression.codec", "snappy").mode("overwrite").parquet(const.dstFolderPath+const.commonneighbourstmpFileName+const.year3)

    /*
    sdf_year1.write.option("sep", "|").mode("overwrite").csv(const.dstFolderPath+const.commonneighbourstmpFileName+const.year1);
    sdf_year2.write.option("sep", "|").mode("overwrite").csv(const.dstFolderPath+const.commonneighbourstmpFileName+const.year1);
    sdf_year3.write.option("sep", "|").mode("overwrite").csv(const.dstFolderPath+const.commonneighbourstmpFileName+const.year1);

     */


  }

  def generateCommonNeighbours(trainingGraph: GraphFrame, testingGraph: GraphFrame, trainingSamples: Dataset[Row],testingSamples: Dataset[Row], const: constantsFile): Unit =
  {
    var tx = System.currentTimeMillis()
    //val gx_year1 = trainingGraph.filterEdges(const.year1_filter).toGraphX;
    //val gx_year2 = testingGraph.filterEdges(const.year2_filter).toGraphX;
    val gx_year1 = trainingGraph.toGraphX;
    val gx_year2 = testingGraph.toGraphX;
    trainingSamples.createOrReplaceGlobalTempView("tr");
    testingSamples.createOrReplaceGlobalTempView("te");

    gx_year1.persist();
    gx_year2.persist();
    //training_samples.persist();
    //testing_samples.persist();

    val intersection = udf((x: Seq[Long], y: Seq[Long]) => x.distinct.intersect(y.distinct))
    spark.udf.register("intersection", intersection);

    val lenx = udf((x: Seq[Long], y: Seq[Long]) => x.distinct.intersect(y.distinct).length)
    spark.udf.register("lenx", lenx);

    println("-------   Count of edges in subgraphs are" + gx_year1.edges.count() + " , " + gx_year2.edges.count() + "   ----------  ");

    val neighbours_year1 = gx_year1.collectNeighborIds(EdgeDirection.Either);
    val neighbours_year2 = gx_year2.collectNeighborIds(EdgeDirection.Either);


    //var op1 = neighbours_year1.filter(line => line._2.length > 0).map(line => line._1 + " | " + line._2.mkString(", "));
    val op1 = neighbours_year1.filter(line => line._2.length > 0).map(row => (row._1, row._2))
    val df_op1 = op1.toDF()
    df_op1.createOrReplaceGlobalTempView("df_op1");
    df_op1.persist();
    df_op1.show(10)

    var interim1 = spark.sql("SELECT t1.src as authorId1, t2._2 as neigbours1, t1.dst as authorId2 FROM global_temp.tr as t1 LEFT OUTER JOIN global_temp.df_op1 as t2 " +
      "ON t1.src = t2._1")

    interim1.createOrReplaceGlobalTempView("i1");

    var interim2 = spark.sql("SELECT DISTINCT t1.authorId1 as authorId1, t1.neigbours1 as neighbours1, t1.authorId2 as authorId2, t2._2 as neighbours2 FROM global_temp.i1 as t1 LEFT OUTER JOIN global_temp.df_op1 as t2 " +
      "ON t1.authorId2 = t2._1")

    interim2.createOrReplaceGlobalTempView("i2");
    interim2.persist()
    interim2.show(10);
    //println(" count of records with neigbours : " + df_op1.count())
    //println(" count of records in interim2 : " + interim2.count() + " | " + spark.sql("SELECT * FROM global_temp.i2 as t1 where t1.neighbours1 is not null and t1.neighbours2 is not null").count())
    spark.sql("SELECT t.authorId1, t.authorId2, lenx(t.neighbours1, t.neighbours2) as common_neighbours,intersection(t.neighbours1, t.neighbours2) as cn_array FROM global_temp.i2 as t where t.neighbours1 is not null and t.neighbours2 is not null").show(100)
    val cn_exists_training = spark.sql("SELECT t.authorId1, t.authorId2, lenx(t.neighbours1, t.neighbours2) as common_neighbours FROM global_temp.i2 as t where t.neighbours1 is not null and t.neighbours2 is not null").repartition(1);
    val cn_notexists_training = spark.sql("SELECT t.authorId1, t.authorId2, 0 as common_neighbours FROM global_temp.i2 as t where t.neighbours1 is  null or t.neighbours2 is null")
    val all_cn_training = cn_notexists_training.union(cn_exists_training)
    all_cn_training.write.option("sep", "|").mode("overwrite").csv(const.dstFolderPath+const.commonneighboursFileName + "training")
    //spark.sql("SELECT * FROM global_temp.i2 as t1 where t1.neighbours1 is not null and t1.neighbours2 is not null").show(100)
    var ty = System.currentTimeMillis()

    //var op2 = neighbours_year2.filter(line => line._2.length > 0).map(line => line._1 + " | " + line._2.mkString(", "));
    var op2 = neighbours_year2.filter(line => line._2.length > 0).map(row => (row._1, row._2));
    var df_op2 = op2.toDF();
    df_op2.createOrReplaceGlobalTempView("df_op2");
    df_op2.persist();
    df_op2.show(10)
    print(" count of records with neigbours : " + df_op2.count());


    var interim3 = spark.sql("SELECT t1.src as authorId1, t2._2 as neigbours1, t1.dst as authorId2 FROM global_temp.te as t1 LEFT OUTER JOIN global_temp.df_op2 as t2 " +
      "ON t1.src = t2._1")

    interim3.createOrReplaceGlobalTempView("i3");

    var interim4 = spark.sql("SELECT DISTINCT t1.authorId1 as authorId1, t1.neigbours1 as neighbours1, t1.authorId2 as authorId2, t2._2 as neighbours2 FROM global_temp.i3 as t1 LEFT OUTER JOIN global_temp.df_op2 as t2 " +
      "ON t1.authorId2 = t2._1")

    interim4.createOrReplaceGlobalTempView("i4");
    interim4.persist()
    interim4.show(10);

    spark.sql("SELECT t.authorId1, t.authorId2, lenx(t.neighbours1, t.neighbours2) as common_neighbours,intersection(t.neighbours1, t.neighbours2) as cn_array FROM global_temp.i4 as t where t.neighbours1 is not null and t.neighbours2 is not null").show(100)
    val cn_exists_testing = spark.sql("SELECT t.authorId1, t.authorId2, lenx(t.neighbours1, t.neighbours2) as common_neighbours FROM global_temp.i4 as t where t.neighbours1 is not null and t.neighbours2 is not null").repartition(1);
    val cn_notexists_testing = spark.sql("SELECT t.authorId1, t.authorId2, 0 as common_neighbours FROM global_temp.i4 as t where t.neighbours1 is  null or t.neighbours2 is null")
    val all_cn_testing = cn_notexists_testing.union(cn_exists_testing)
    all_cn_testing.write.option("sep", "|").mode("overwrite").csv(const.dstFolderPath+const.commonneighboursFileName + "testing")
    var tz = System.currentTimeMillis()

    println("- count of training and testing data is: " + all_cn_training.count + " | " + all_cn_testing.count())
    println("Time taken to create training and testing common neighbours dataset are: " + (ty-tx) + " | " + (tz-ty) + " milliseconds")

  }

  def generateCommonNeighbours2(trainingGraph: GraphFrame, testingGraph: GraphFrame, trainingSamples: Dataset[Row],testingSamples: Dataset[Row],  const: constantsFile): Unit =
  {
    //For training data
    var trainingStart = System.currentTimeMillis();
    trainingGraph.edges.createOrReplaceGlobalTempView("training_edges")
    trainingSamples.createOrReplaceGlobalTempView("training_samples")

    var interim1 = spark.sql("WITH t1 AS (SELECT x.src, x.dst  FROM global_temp.training_edges as x inner join global_temp.training_samples as y on x.src = y.src), " +
      "t2 AS (SELECT x.src, x.dst FROM global_temp.training_edges as x inner join global_temp.training_samples as y on x.dst = y.dst) " +
      "SELECT distinct t1.src as authorId1, t1.dst as neighbour, t2.dst as authorId2 FROM t1 inner join t2 on t1.dst = t2.src where t1.src <> t2.dst")

    print("count of interim1 is: " + interim1.count())
    interim1.createOrReplaceGlobalTempView("interim1")
    interim1.show(10);
    //interim1.write.option("sep", "|").mode("overwrite").csv(const.dstFolderPath+const.commonneighbourstmpFileName)

    var interim2 = spark.sql("SELECT t1.src as src, t1.dst as dst, (CASE WHEN t2.authorId1 IS NULL THEN 0 ELSE 1 END) as neighbour, t2.neighbour as x  " +
      "FROM global_temp.training_samples as t1 left outer join global_temp.interim1 as t2 on t1.src = t2.authorId1 " +
      "and t1.dst = t2.authorId2")
    interim2.createOrReplaceGlobalTempView("interim2")


    //interim2.orderBy(desc("neighbour")).show(100)
    var trainingResult = spark.sql("SELECT t1.src, t1.dst, sum(neighbour) as common_neighbours from global_temp.interim2 as t1 group by t1.src, t1.dst")

    //trainingResult.show(10)
    println("count of training result is : " + trainingResult.count())

    trainingResult.orderBy(desc("common_neighbours")).show(100)

    trainingResult.write.option("sep", "|").mode("overwrite").csv(const.dstFolderPath+const.commonneighboursFileName + const.year1)
    var trainingEnd = System.currentTimeMillis()

    println("time taken for training data computation: " + (trainingEnd - trainingStart)/1000 + " seconds ");

    var testingStart = System.currentTimeMillis();
    testingGraph.edges.createOrReplaceGlobalTempView("testing_edges")
    testingSamples.createOrReplaceGlobalTempView("testing_samples")

    var interim3 = spark.sql("WITH t1 AS (SELECT x.src, x.dst  FROM global_temp.testing_edges as x inner join global_temp.testing_samples as y on x.src = y.src), " +
      "t2 AS (SELECT x.src, x.dst FROM global_temp.testing_edges as x inner join global_temp.testing_samples as y on x.dst = y.dst) " +
      "SELECT distinct t1.src as authorId1, t1.dst as neighbour, t2.dst as authorId2 FROM t1 inner join t2 on t1.dst = t2.src where t1.src <> t2.dst")

    print("count of interim3 is: " + interim3.count())
    interim3.createOrReplaceGlobalTempView("interim3")
    interim3.show(10);
    //interim1.write.option("sep", "|").mode("overwrite").csv(const.dstFolderPath+const.commonneighbourstmpFileName)

    var interim4 = spark.sql("SELECT t1.src as src, t1.dst as dst, (CASE WHEN t2.authorId1 IS NULL THEN 0 ELSE 1 END) as neighbour, t2.neighbour as x  " +
      "FROM global_temp.testing_samples as t1 left outer join global_temp.interim3 as t2 on t1.src = t2.authorId1 " +
      "and t1.dst = t2.authorId2")
    interim4.createOrReplaceGlobalTempView("interim2")


    //interim4.orderBy(desc("neighbour")).show(100)
    var testingResult = spark.sql("SELECT t1.src, t1.dst, sum(neighbour) as common_neighbours from global_temp.interim4 as t1 group by t1.src, t1.dst")

    //testingResult.show(10)
    println("count of training result is : " + testingResult.count())

    testingResult.orderBy(desc("common_neighbours")).show(100)

    testingResult.write.option("sep", "|").mode("overwrite").csv(const.dstFolderPath+const.commonneighboursFileName + const.year2)
    var testingEnd = System.currentTimeMillis()

    println("time taken for training data computation: " + (testingEnd - testingStart)/1000 + " seconds ");
  }

}
