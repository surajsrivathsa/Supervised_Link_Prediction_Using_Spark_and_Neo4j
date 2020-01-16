package pkg

import org.apache.spark
import org.apache.spark.sql
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.graphframes._

class Step2_Linkexistence(spark: SparkSession) {

  def generateLinkExistsSamples( graph: GraphFrame, const: constantsFile) : Unit =
    {
      var edf_year1 = graph.edges.select("src","dst","labelYear").withColumn("linkexists", lit("yes")).where("src < dst").where(const.s2f_year1_filter).orderBy(rand()).limit(const.existscount)
      var edf_year2 = graph.edges.select("src","dst","labelYear").withColumn("linkexists", lit("yes")).where("src < dst").where(const.s2f_year2_filter).orderBy(rand()).limit(const.existscount)
      var edf_year3 = graph.edges.select("src","dst","labelYear").withColumn("linkexists", lit("yes")).where("src < dst").where(const.s2f_year3_filter).orderBy(rand()).limit(const.existscount)
      edf_year1.show(20)
      edf_year1.write.option("sep", "|").mode("overwrite").csv(const.dstFolderPath+const.s2f_training_FileName+const.year1);
      edf_year2.write.option("sep", "|").mode("overwrite").csv(const.dstFolderPath+const.s2f_training_FileName+const.year2);
      edf_year3.write.option("sep", "|").mode("overwrite").csv(const.dstFolderPath+const.s2f_training_FileName+const.year3);
    }

  def generateSamples( graph: GraphFrame, const:constantsFile) : Array[Dataset[Row]] =
  {
    var tx = System.currentTimeMillis();

    var a1Df = graph.vertices.toDF("id","label").select("id" ).withColumnRenamed("id", "authorId1").orderBy(rand()).limit(const.notexistscount);
    var a2Df = graph.vertices.toDF("id","label").select("id").withColumnRenamed("id", "authorId2").orderBy(rand()).limit(const.notexistscount);
    a1Df.persist();
    a2Df.persist();
    println("----------- a1df and a2df --------------")
    a1Df.show(10); a2Df.show(10);
    var authortupleDf = a1Df.join(a2Df, a1Df("authorId1")  < a2Df("authorId2") , "inner");


    authortupleDf.persist();
    graph.edges.persist();

    authortupleDf.show(10)
    println(" ----------------  Count of tuples are: " + authortupleDf.count() + "   ---------------");

    graph.edges.createOrReplaceGlobalTempView("t1");
    authortupleDf.createOrReplaceGlobalTempView("t2");

    var authortuplesWithNoLinksInAllYears = spark.sql("select distinct t2.authorId1 as src, t2.authorId2 as dst, t1.year as year from global_temp.t2 as t2 left outer join global_temp.t1 as t1 on t1.src = t2.authorId1 and t1.dst = t2.authorId2 where t1.src is null and t1.dst is null");

    var year3 = const.year3; var year2 = const.year2; var year1 = const.year1;

    var edges_year3 = spark.sql(s"select * from global_temp.t1 where t1.year = $year3 and t1.src < t1.dst")
    var edges_year2 = spark.sql(s"select * from global_temp.t1 where t1.year = $year2 and t1.src < t1.dst")
    var edges_year1 = spark.sql(s"select * from global_temp.t1 where t1.year = $year1 and t1.src < t1.dst")

    edges_year3.createOrReplaceGlobalTempView("e3");
    edges_year2.createOrReplaceGlobalTempView("e2");
    edges_year1.createOrReplaceGlobalTempView("e1")

    var authortuplesof_Year3_Notin_other_Years = spark.sql("select distinct e3.src, e3.dst, e3.year   from global_temp.e3 as e3 left outer join " +
      "(select * from global_temp.e2 union select * from global_temp.e1) as e0 " +
      "on e3.src = e0.src and e3.dst = e0.dst where e0.src is null and e0.dst is null" ) ;

    var authortuplesof_Year2_Notin_Year1 = spark.sql("select distinct e2.src, e2.dst, e2.year from global_temp.e2 as e2 left outer join " +
      "global_temp.e1 as e1 on e2.src = e1.src and e2.dst = e1.dst " +
      "where e1.src is null and e1.dst is null");

    var authortuplesofboth_Year2_and_Year1 = spark.sql("select distinct e2.src, e2.dst, e2.year  from global_temp.e2 as e2 inner join " +
    "global_temp.e1 as e1 on e2.src = e1.src and e2.dst = e1.dst ");

    var authortuplesofboth_Year3_and_others = spark.sql("select distinct e3.src, e3.dst, e3.year from global_temp.e3 as e3 inner join " +
      "global_temp.e2 as e2 on e2.src = e3.src and e2.dst = e3.dst inner join global_temp.e1 as e1 on " +
      "e1.src = e3.src and e1.dst = e3.dst");

    var authortuplesof_Year1_and_2_Notin_Year3 = spark.sql("select distinct e0.src, e0.dst, e0.year   from global_temp.e3 as e3 right outer join " +
      "(select * from global_temp.e2 union select * from global_temp.e1) as e0 " +
      "on e3.src = e0.src and e3.dst = e0.dst where e3.src is null and e3.dst is null" ) ;

    authortuplesWithNoLinksInAllYears.createOrReplaceGlobalTempView("x1")
    authortuplesof_Year3_Notin_other_Years.createOrReplaceGlobalTempView("x2");
    authortuplesof_Year2_Notin_Year1.createOrReplaceGlobalTempView("x3");

    //var samples = authortuplesWithNoLinksInAllYears.union(authortuplesof_Year3_Notin_other_Years).union(authortuplesof_Year2_Notin_Year1).orderBy(rand());

    println("-------- count of authortuplesof_Year3_Notin_other_Years is: " + authortuplesof_Year3_Notin_other_Years.count());

    var linknotexists_samples_year2 = authortuplesWithNoLinksInAllYears.limit(500000).union(authortuplesof_Year3_Notin_other_Years.limit(500000)).withColumn("linkexists", lit(0)).orderBy(rand());
    var linkexists_samples_year2 = edges_year2.select("src", "dst", "year").distinct().withColumn("linkexists", lit(1)).orderBy(rand()).limit(1000000);
    var interim1 = linknotexists_samples_year2.union(linkexists_samples_year2)
    //trainingData.persist();
    interim1.createOrReplaceGlobalTempView("t")
    spark.sql("select count(*), src, dst from global_temp.t group by t.src, t.dst having count(*) > 1").show(10);
    println(" -----------  my count: " + spark.sql("select count(*), src, dst from global_temp.t group by t.src, t.dst having count(*) > 1").count());
    var trainingData = spark.sql( "SELECT DISTINCT t.src, t.dst, t.year, t.linkexists from global_temp.t")
    trainingData.createOrReplaceGlobalTempView("t1")
    trainingData.persist();
    trainingData.write.option("sep", "|").mode("overwrite").csv(const.dstFolderPath+const.s2f_training_FileName);
    var ty = System.currentTimeMillis()

    var linkexists_samples_year3 = edges_year3.select("src", "dst", "year").distinct().withColumn("linkexists", lit(1)).orderBy(rand()).limit(1000000);
    var link_notexists_year3a = authortuplesWithNoLinksInAllYears.orderBy(rand(123L)).limit(500000)
    var link_notexists_year3b = authortuplesof_Year1_and_2_Notin_Year3.orderBy(rand(1L)).limit(500000)
    var link_notexists_year3 = link_notexists_year3a.union(link_notexists_year3b).withColumn("linkexists", lit(0)).orderBy(rand());
    var testingData = linkexists_samples_year3.union(link_notexists_year3)
    testingData.persist();
    testingData.write.option("sep", "|").mode("overwrite").csv(const.dstFolderPath+const.s2f_validation_FileName)

    var tz = System.currentTimeMillis()
    /*
    val linkexistence_path = (1 to 1).map(i => s"(n$i)-[e$i]->(n${i + 1})").mkString("")
    println(linkexistence_path);
    val linkexistence_filter = (1 to 2).map(i => s"n$i").combinations(2).map { case Seq(i, j) => col(i) !== col(j) }.reduce(_ && _)
    print(linkexistence_filter);

    println(" ---------- the filters and paths are: " + linkexistence_path + "    ------ " + linkexistence_filter.toString());

    var sdf = graph.find(linkexistence_path).filter(linkexistence_filter).filter("e1.year <= 1990");
    sdf.persist();
    println( "SDF count is ---------  " + sdf.count());
    sdf.show(10);
    var tdf = sdf.withColumnRenamed("n1.id", "authorId1");
    var xdf = tdf.withColumnRenamed("n2.id", "authorId2").select("authorId1", "authorId2");
    xdf.show(10)
     */
    //graph.bfs.
    println("Count of training and testing samples are: " + trainingData.count() + " | " + testingData.count())
    println("Time taken to create training and testing dataset are: " + (ty-tx) + " | " + (tz-ty) + " milliseconds")
    return Array(trainingData, testingData);
  }

  def generateSubGraphs(graph: GraphFrame, const: constantsFile): Array[GraphFrame] =
  {
    val graph_year1 = graph.filterEdges(const.year1_filter);
    val graph_year2 = graph.filterEdges(const.year2_filter);
    //val graph_year3 = graph.filterEdges(const.year3_filter);

    graph_year1.persist(); graph_year2.persist();

    print(graph_year1.edges.count() + " ---- " + graph_year2.edges.count())
    return Array(graph_year1, graph_year2)
  }

  def generateSamplesFromFile(const: constantsFile): Array[Dataset[Row]] = {

    val trainingSamples = spark.read.schema(const.authorTupleSchema).option("sep", "|").option("header", "true").option("inferSchema", "false").csv(const.dstFolderPath+const.s2f_training_FileName + "/*");
    val testingSamples = spark.read.schema(const.authorTupleSchema).option("sep", "|").option("header", "true").option("inferSchema", "false").csv(const.dstFolderPath+const.s2f_validation_FileName + "/*");
    trainingSamples.show(10); testingSamples.show(10);

    return Array(trainingSamples, testingSamples)
  }
}
