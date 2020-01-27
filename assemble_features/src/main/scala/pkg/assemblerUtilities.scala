package pkg

import org.apache.spark.sql.SparkSession

class assemblerUtilities(spark: SparkSession) {

  def assembleTrainingfeatures(const : constantsFile) : Unit =
  {

    var authorTupledf_training = spark.read.schema(const.authorTupleSchema).format("csv").option("sep","|").option("header", "false").option("mode", "DROPMALFORMED").option("inferSchema", "false").load(const.dstFolderPath+const.trainingsamplesFolder+"/*");
    authorTupledf_training.persist();
    authorTupledf_training.show(10);

    var dynamicpagerankdf = spark.read.schema(const.pagerankSchema).format("csv").option("sep","|").option("header", "false").option("mode", "DROPMALFORMED").option("inferSchema", "false").load(const.dstFolderPath+const.trainingglobalpagerankFolder+ "/*");
    dynamicpagerankdf.persist();
    dynamicpagerankdf.show(10)

    var pagerankdf = spark.read.schema(const.pagerankSchema).format("csv").option("sep","|").option("header", "false").option("mode", "DROPMALFORMED").option("inferSchema", "false").load(const.dstFolderPath+const.trainingpagerankFolder + "/*");
    pagerankdf.persist();
    pagerankdf.show(10)

    var padf = spark.read.schema(const.preferentialattachmentSchema).format("csv").option("sep","|").option("header", "false").option("mode", "DROPMALFORMED").option("inferSchema", "false").load(const.dstFolderPath + const.trainingglobalprefattachFolder + "/*");
    padf.persist();
    padf.show(10)

    var cndf = spark.read.schema(const.commonneighboursSchema).format("csv").option("sep","|").option("header", "false").option("mode", "DROPMALFORMED").option("inferSchema", "false").load(const.dstFolderPath + const.trainingcnFolder + "/*");
    cndf.persist();
    cndf.show(10)


    var fspdf = spark.read.schema(const.firstshortestpathSchema).format("csv").option("sep","|").option("header", "false").option("mode", "DROPMALFORMED").option("inferSchema", "false").load(const.dstFolderPath + const.trainingfspFolder + "/*");
    fspdf.persist();
    fspdf.show(10)

    var sspdf = spark.read.schema(const.secondshortestpathSchema).format("csv").option("sep","|").option("header", "false").option("mode", "DROPMALFORMED").option("inferSchema", "false").load(const.dstFolderPath + const.trainingsspFolder + "/*");
    sspdf.persist();
    sspdf.show(10)


    authorTupledf_training.createOrReplaceGlobalTempView("at");
    dynamicpagerankdf.createOrReplaceGlobalTempView("dr");
    pagerankdf.createOrReplaceGlobalTempView("pr");
    padf.createOrReplaceGlobalTempView("padf");
    cndf.createOrReplaceGlobalTempView("cndf");
    fspdf.createOrReplaceGlobalTempView("fspdf");
    sspdf.createOrReplaceGlobalTempView("sspdf");

    var featureset0df = spark.sql("select t1.*, coalesce(pagerankscore, 0.0) as dynamic_pagerankscore1 from global_temp.at as t1 left outer join global_temp.dr as pr1 on t1.authorId1 = pr1.vertexid ");

    featureset0df.createOrReplaceGlobalTempView("d1");
    featureset0df.show(10)


    var featureset0xdf = spark.sql("select t1.*, coalesce(pr1.pagerankscore, 0.0) as dynamic_pagerankscore2 from global_temp.d1 as t1 left outer join global_temp.dr as pr1 on t1.authorId2 = pr1.vertexid ");

    featureset0xdf.createOrReplaceGlobalTempView("d2");
    featureset0xdf.show(10)

    //var linexists = spark.sql("select * from global_temp.at as t where t")

    var featureset1df = spark.sql("select t1.*, coalesce(pr1.pagerankscore,0.0) as pagerankscore1 from global_temp.d2 as t1 left outer join global_temp.pr as pr1 on t1.authorId1 = pr1.vertexid ");

    featureset1df.createOrReplaceGlobalTempView("f1");

    var featureset2df = spark.sql("select t1.*, coalesce(pr1.pagerankscore,0.0) as pagerankscore2 from global_temp.f1 as t1 left outer join global_temp.pr as pr1 on t1.authorId2 = pr1.vertexid ");


    featureset2df.show(20);
    println("the featureset2df count is: " + featureset2df.count());
    featureset2df.createOrReplaceGlobalTempView("f2");
    //featureset2df.write.csv("/Users/surajshashidhar/Desktop/graphml_10lkh/featureset2");

    var featureset3df = spark.sql("select f2.*, coalesce(padf.preferential_attachment_score , 0.0 ) as preferential_attachment_score1 from global_temp.f2 as f2 left outer join global_temp.padf as padf on f2.authorId1 = padf.vertexid");
    featureset3df.createOrReplaceGlobalTempView("f3");
    println("the featureset3df count is: " + featureset3df.count());


    var featureset4df = spark.sql("select f3.*, coalesce(padf.preferential_attachment_score , 0.0 )  as preferential_attachment_score2 from global_temp.f3 as f3 left outer join global_temp.padf as padf on f3.authorId2 = padf.vertexid");
    featureset4df.createOrReplaceGlobalTempView("f4")
    println("the featureset4df count is: " + featureset4df.count());

    var featureset5df = spark.sql("SELECT  f4.*, coalesce(t.common_neighbours, 0) as common_neighbours FROM global_temp.f4 as f4 LEFT OUTER  join global_temp.cndf as t on f4.authorId1 = t.authorId1 and f4.authorId2 = t.authorId2")
    featureset5df.createOrReplaceGlobalTempView("f5");
    println("the featureset5df count is: " + featureset5df.count());

    var featureset6df = spark.sql("SELECT  f5.*, coalesce(t.distance , 0.0 ) as first_shortest_path  FROM global_temp.f5 as f5 LEFT OUTER JOIN global_temp.fspdf as t on f5.authorId1 = t.src and f5.authorId2 = t.dst")
    featureset6df.createOrReplaceGlobalTempView("f6");
    println("the featureset6df count is: " + featureset6df.count())

    var featureset7df = spark.sql("SELECT  f6.*, coalesce(t.distance2 , 0.0 ) as second_shortest_path  FROM global_temp.f6 as f6 INNER JOIN global_temp.sspdf as t on f6.authorId1 = t.src and f6.authorId2 = t.dst")
    println("the featureset7df count is: " + featureset7df.count());


    //featureset5df.write.mode("overwrite").option("sep", "|").option("header", "true").csv(const.dstFolderPath +const.trainfeaturesetFileName);
    //println("count at featureset6 is: " + featureset6df.count())
    featureset7df.write.mode("overwrite").option("sep", "|").option("header", "true").csv(const.dstFolderPath +const.trainingfeaturesFolder);
    featureset7df.show(30);
    println("Count of training dataset: " + featureset7df.count());
  }


  def assembleTestingFeatures(const: constantsFile): Unit =
  {

    var authorTupledf_testing = spark.read.schema(const.authorTupleSchema).format("csv").option("sep","|").option("header", "false").option("mode", "DROPMALFORMED").option("inferSchema", "false").load(const.dstFolderPath+const.testingsamplesFolder+"/*");
    authorTupledf_testing.persist();
    authorTupledf_testing.show(10);

    var dynamicpagerankdf = spark.read.schema(const.pagerankSchema).format("csv").option("sep","|").option("header", "false").option("mode", "DROPMALFORMED").option("inferSchema", "false").load(const.dstFolderPath+const.testingglobalpagerankFolder + "/*");
    dynamicpagerankdf.persist();
    dynamicpagerankdf.show(10)


    var pagerankdf = spark.read.schema(const.pagerankSchema).format("csv").option("sep","|").option("header", "false").option("mode", "DROPMALFORMED").option("inferSchema", "false").load(const.dstFolderPath+const.testingpagerankFolder + "/*");
    pagerankdf.persist();
    pagerankdf.show(10)

    var padf = spark.read.schema(const.preferentialattachmentSchema).format("csv").option("sep","|").option("header", "false").option("mode", "DROPMALFORMED").option("inferSchema", "false").load(const.dstFolderPath + const.testingglobalprefattachFolder + "/*");
    padf.persist();
    padf.show(10)

    var cndf = spark.read.schema(const.commonneighboursSchema).format("csv").option("sep","|").option("header", "false").option("mode", "DROPMALFORMED").option("inferSchema", "false").load(const.dstFolderPath + const.testingcnFolder + "/*");
    cndf.persist();
    cndf.show(10)

    var fspdf = spark.read.schema(const.firstshortestpathSchema).format("csv").option("sep","|").option("header", "false").option("mode", "DROPMALFORMED").option("inferSchema", "false").load(const.dstFolderPath + const.testingfspFolder + "/*");
    fspdf.persist();
    fspdf.show(10)


    var sspdf = spark.read.schema(const.secondshortestpathSchema).format("csv").option("sep","|").option("header", "false").option("mode", "DROPMALFORMED").option("inferSchema", "false").load(const.dstFolderPath + const.testingsspFolder + "/*");
    sspdf.persist();
    sspdf.show(10)

    authorTupledf_testing.createOrReplaceGlobalTempView("at");
    dynamicpagerankdf.createOrReplaceGlobalTempView("dr");;
    pagerankdf.createOrReplaceGlobalTempView("pr");
    padf.createOrReplaceGlobalTempView("padf");
    cndf.createOrReplaceGlobalTempView("cndf")
    fspdf.createOrReplaceGlobalTempView("fspdf")
    sspdf.createOrReplaceGlobalTempView("sspdf")

    var featureset0df = spark.sql("select t1.*, coalesce(pagerankscore, 0.0) as dynamic_pagerankscore1 from global_temp.at as t1 left outer join global_temp.dr as pr1 on t1.authorId1 = pr1.vertexid ");

    featureset0df.createOrReplaceGlobalTempView("d1");
    featureset0df.show(10)


    var featureset0xdf = spark.sql("select t1.*, coalesce(pr1.pagerankscore, 0.0) as dynamic_pagerankscore2 from global_temp.d1 as t1 left outer join global_temp.dr as pr1 on t1.authorId2 = pr1.vertexid ");

    featureset0xdf.createOrReplaceGlobalTempView("d2");
    featureset0xdf.show(10)
    //var linexists = spark.sql("select * from global_temp.at as t where t")

    var featureset1df = spark.sql("select t1.*, coalesce(pr1.pagerankscore, 0.0) as pagerankscore1 from global_temp.d2 as t1 left outer join global_temp.pr as pr1 on t1.authorId1 = pr1.vertexid ");

    featureset1df.createOrReplaceGlobalTempView("f1");

    var featureset2df = spark.sql("select t1.*, coalesce(pr1.pagerankscore, 0.0) as pagerankscore2 from global_temp.f1 as t1 left outer join global_temp.pr as pr1 on t1.authorId2 = pr1.vertexid ");


    featureset2df.show(20);
    println("the count of records is: " + featureset2df.count());
    featureset2df.createOrReplaceGlobalTempView("f2");
    //featureset2df.write.csv("/Users/surajshashidhar/Desktop/graphml_10lkh/featureset2");

    var featureset3df = spark.sql("select f2.*, coalesce(padf.preferential_attachment_score , 0.0 ) as preferential_attachment_score1 from global_temp.f2 as f2 left outer join global_temp.padf as padf on f2.authorId1 = padf.vertexid");
    featureset3df.createOrReplaceGlobalTempView("f3");

    var featureset4df = spark.sql("select f3.*, coalesce(padf.preferential_attachment_score , 0.0 )  as preferential_attachment_score2 from global_temp.f3 as f3 left outer join global_temp.padf as padf on f3.authorId2 = padf.vertexid");
    featureset4df.createOrReplaceGlobalTempView("f4")

    var featureset5df = spark.sql("SELECT DISTINCT f4.*, coalesce(t.common_neighbours, 0) as common_neighbours FROM global_temp.f4 as f4 left outer join global_temp.cndf as t on f4.authorId1 = t.authorId1 and f4.authorId2 = t.authorId2")

    featureset5df.createOrReplaceGlobalTempView("f5");
    println(featureset5df.count() + "  --- ")

    var featureset6df = spark.sql("SELECT DISTINCT f5.*, coalesce(t.distance , 0.0 ) as first_shortest_path  FROM global_temp.f5 as f5 LEFT OUTER JOIN global_temp.fspdf as t on f5.authorId1 = t.src and f5.authorId2 = t.dst")
    featureset6df.createOrReplaceGlobalTempView("f6");
    println("the featureset6df count is: " + featureset6df.count())

    var featureset7df = spark.sql("SELECT  f6.*, coalesce(t.distance2 , 0.0 ) as second_shortest_path  FROM global_temp.f6 as f6 INNER JOIN global_temp.sspdf as t on f6.authorId1 = t.src and f6.authorId2 = t.dst")
    println("the featureset7df count is: " + featureset7df.count());

    featureset7df.write.mode("overwrite").option("sep", "|").option("header", "true").csv(const.dstFolderPath +const.testingfeaturesFolder);
    featureset7df.show(30);
    println("Count of testing features: " + featureset7df.count());

  }


}
