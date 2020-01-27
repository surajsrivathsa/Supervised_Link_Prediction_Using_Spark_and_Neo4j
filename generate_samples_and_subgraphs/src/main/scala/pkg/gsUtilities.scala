package pkg

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel
import org.graphframes.GraphFrame

class gsUtilities(spark: SparkSession) {
  import spark.implicits._

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


  def generateSamples( graph: GraphFrame, const:constantsFile) : Unit =
  {
    var tx = System.currentTimeMillis();

    var a1Df = graph.vertices.toDF("id","label").select("id" ).withColumnRenamed("id", "authorId1").orderBy(rand()).limit(const.randomVerticesCount);
    var a2Df = graph.vertices.toDF("id","label").select("id").withColumnRenamed("id", "authorId2").orderBy(rand()).limit(const.randomVerticesCount);
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

    var linknotexists_samples_year2 = authortuplesWithNoLinksInAllYears.orderBy(rand()).limit(const.sampleNotExistsCount).union(authortuplesof_Year3_Notin_other_Years.orderBy(rand()).limit(const.sampleNotExistsCount)).withColumn("linkexists", lit(0));
    var linkexists_samples_year2 = edges_year2.select("src", "dst", "year").distinct().withColumn("linkexists", lit(1)).orderBy(rand()).limit(const.sampleExistsCount);
    var interim1 = linknotexists_samples_year2.union(linkexists_samples_year2)
    //trainingData.persist();
    interim1.createOrReplaceGlobalTempView("t")
    spark.sql("select count(*), src, dst from global_temp.t group by t.src, t.dst having count(*) > 1").show(10);
    println(" -----------  my count: " + spark.sql("select count(*), src, dst from global_temp.t group by t.src, t.dst having count(*) > 1").count());
    var trainingData = spark.sql( "SELECT DISTINCT t.src, t.dst, t.year, t.linkexists from global_temp.t")
    trainingData.createOrReplaceGlobalTempView("t1")
    trainingData.persist();
    trainingData.write.option("sep", "|").mode("overwrite").csv(const.dstFolderPath+const.trainingsamplesFolder);
    var ty = System.currentTimeMillis()

    var linkexists_samples_year3 = edges_year3.select("src", "dst", "year").distinct().withColumn("linkexists", lit(1)).orderBy(rand()).limit(const.sampleExistsCount);
    var link_notexists_year3a = authortuplesWithNoLinksInAllYears.orderBy(rand(123L)).limit(const.sampleNotExistsCount)
    var link_notexists_year3b = authortuplesof_Year1_and_2_Notin_Year3.orderBy(rand(45L)).limit(const.sampleNotExistsCount)
    var link_notexists_year3 = link_notexists_year3a.union(link_notexists_year3b).withColumn("linkexists", lit(0)).orderBy(rand());
    var testingData = linkexists_samples_year3.union(link_notexists_year3)
    testingData.persist();
    testingData.write.option("sep", "|").mode("overwrite").csv(const.dstFolderPath+const.testingsamplesFolder)

    var tz = System.currentTimeMillis()
    println("Count of training and testing samples are: " + trainingData.count() + " | " + testingData.count())
    println("Time taken to create training and testing dataset are: " + (ty-tx)/1000 + " | " + (tz-ty)/1000 + "  seconds")

    graph.edges.unpersist();
    return ;
  }


  def generateSubGraphs(graph: GraphFrame, const: constantsFile): Unit =
  {
    val graph_year1 = graph.filterEdges(const.year1_filter);
    val graph_year2 = graph.filterEdges(const.year2_filter);
    //val graph_year3 = graph.filterEdges(const.year3_filter);

    graph_year1.persist(); graph_year2.persist();

    println("Count of edges in training and testing graph: " + graph_year1.edges.count() + " ---- " + graph_year2.edges.count())

    graph_year1.vertices.write.option("spark.sql.parquet.compression.codec","snappy").mode("overwrite").parquet(const.dstFolderPath + const.trainingsubgraphverticesFolder)
    graph_year2.vertices.write.option("spark.sql.parquet.compression.codec","snappy").mode("overwrite").parquet(const.dstFolderPath + const.testingsubgraphverticesFolder)

    graph_year1.edges.write.option("spark.sql.parquet.compression.codec","snappy").mode("overwrite").parquet(const.dstFolderPath + const.trainingsubgraphedgesFolder)
    graph_year2.edges.write.option("spark.sql.parquet.compression.codec","snappy").mode("overwrite").parquet(const.dstFolderPath + const.testingsubgraphedgesFolder)

    return;
  }


}
