package pkg

import org.apache.spark.sql._
import org.apache.spark.sql.types._


class assembleFeatures(spark: SparkSession)  {

  def assembleTrainingfeatures(const : constantsFile) : Unit =
  {
    val authorTupleSchema = StructType(Array(StructField("authorId1", LongType, nullable = false), StructField("authorId2", LongType, nullable = false),StructField("year", IntegerType, nullable = true),StructField("expected_label", IntegerType, nullable = false)))

    val pagerankSchema = StructType(Array(StructField("vertexid", LongType, nullable = false), StructField("pagerankscore", DoubleType, nullable = false)))

    val preferentialattachmentSchema = StructType(Array(StructField("preferential_attachment_score", DoubleType, nullable = false), StructField("numberofconnectednodes", IntegerType, nullable = false),StructField("vertexid", LongType, nullable = false),StructField("year", IntegerType, nullable = true)))

    var authorTupledf_training = spark.read.schema(authorTupleSchema).format("csv").option("sep","|").option("header", "false").option("mode", "DROPMALFORMED").option("inferSchema", "false").load(const.dstFolderPath+const.s2f_training_FileName+"/*");
    authorTupledf_training.persist();
    authorTupledf_training.show(10);

    var pagerankdf = spark.read.schema(pagerankSchema).format("csv").option("header", "false").option("mode", "DROPMALFORMED").option("inferSchema", "false").load(const.dstFolderPath+const.pagerankFileName+const.year1 + "/*");
    pagerankdf.persist();
    pagerankdf.show(10)

    var padf = spark.read.schema(preferentialattachmentSchema).format("csv").option("sep","|").option("header", "false").option("mode", "DROPMALFORMED").option("inferSchema", "false").load(const.dstFolderPath + const.prefattachFileName + const.year1+ "/*");
    padf.persist();
    padf.show(10)

    authorTupledf_training.createOrReplaceGlobalTempView("at");
    pagerankdf.createOrReplaceGlobalTempView("pr");
    padf.createOrReplaceGlobalTempView("padf");

    //var linexists = spark.sql("select * from global_temp.at as t where t")

    var featureset1df = spark.sql("select t1.*, pr1.pagerankscore as pagerankscore1 from global_temp.at as t1 inner join global_temp.pr as pr1 on t1.authorId1 = pr1.vertexid ");

    featureset1df.createOrReplaceGlobalTempView("f1");

    var featureset2df = spark.sql("select t1.*, pr1.pagerankscore as pagerankscore2 from global_temp.f1 as t1 inner join global_temp.pr as pr1 on t1.authorId2 = pr1.vertexid ");


    featureset2df.show(20);
    println("the count of records is: " + featureset2df.count());
    featureset2df.createOrReplaceGlobalTempView("f2");
    //featureset2df.write.csv("/Users/surajshashidhar/Desktop/graphml_10lkh/featureset2");

    var featureset3df = spark.sql("select f2.*, coalesce(padf.preferential_attachment_score , 0.0 ) as preferential_attachment_score1 from global_temp.f2 as f2 inner join global_temp.padf as padf on f2.authorId1 = padf.vertexid");
    featureset3df.createOrReplaceGlobalTempView("f3");

    var featureset4df = spark.sql("select f3.*, coalesce(padf.preferential_attachment_score , 0.0 )  as preferential_attachment_score2 from global_temp.f3 as f3 inner join global_temp.padf as padf on f3.authorId2 = padf.vertexid");
    featureset4df.write.mode("overwrite").option("sep", "|").option("header", "true").csv(const.dstFolderPath +const.trainfeaturesetFileName);


    featureset4df.show(30);
    println(featureset4df.count());
  }


  def assembleValidationFeatures(const: constantsFile): Unit =
  {
    val authorTupleSchema = StructType(Array(StructField("authorId1", LongType, nullable = false), StructField("authorId2", LongType, nullable = false),StructField("year", IntegerType, nullable = true),StructField("expected_label", IntegerType, nullable = false)))

    val pagerankSchema = StructType(Array(StructField("vertexid", LongType, nullable = false), StructField("pagerankscore", DoubleType, nullable = false)))

    val preferentialattachmentSchema = StructType(Array(StructField("preferential_attachment_score", DoubleType, nullable = false), StructField("numberofconnectednodes", IntegerType, nullable = false),StructField("vertexid", LongType, nullable = false),StructField("year", IntegerType, nullable = true)))

    var authorTupledf_training = spark.read.schema(authorTupleSchema).format("csv").option("sep","|").option("header", "false").option("mode", "DROPMALFORMED").option("inferSchema", "false").load(const.dstFolderPath+const.s2f_validation_FileName+"/*");
    authorTupledf_training.persist();
    authorTupledf_training.show(10);

    var pagerankdf = spark.read.schema(pagerankSchema).format("csv").option("header", "false").option("mode", "DROPMALFORMED").option("inferSchema", "false").load(const.dstFolderPath+const.pagerankFileName+const.year2 + "/*");
    pagerankdf.persist();
    pagerankdf.show(10)

    var padf = spark.read.schema(preferentialattachmentSchema).format("csv").option("sep","|").option("header", "false").option("mode", "DROPMALFORMED").option("inferSchema", "false").load(const.dstFolderPath + const.prefattachFileName + const.year2 + "/*");
    padf.persist();
    padf.show(10)

    authorTupledf_training.createOrReplaceGlobalTempView("at");
    pagerankdf.createOrReplaceGlobalTempView("pr");
    padf.createOrReplaceGlobalTempView("padf");

    //var linexists = spark.sql("select * from global_temp.at as t where t")

    var featureset1df = spark.sql("select t1.*, pr1.pagerankscore as pagerankscore1 from global_temp.at as t1 inner join global_temp.pr as pr1 on t1.authorId1 = pr1.vertexid ");

    featureset1df.createOrReplaceGlobalTempView("f1");

    var featureset2df = spark.sql("select t1.*, pr1.pagerankscore as pagerankscore2 from global_temp.f1 as t1 inner join global_temp.pr as pr1 on t1.authorId2 = pr1.vertexid ");


    featureset2df.show(20);
    println("the count of records is: " + featureset2df.count());
    featureset2df.createOrReplaceGlobalTempView("f2");
    //featureset2df.write.csv("/Users/surajshashidhar/Desktop/graphml_10lkh/featureset2");

    var featureset3df = spark.sql("select f2.*, coalesce(padf.preferential_attachment_score , 0.0 ) as preferential_attachment_score1 from global_temp.f2 as f2 inner join global_temp.padf as padf on f2.authorId1 = padf.vertexid");
    featureset3df.createOrReplaceGlobalTempView("f3");

    var featureset4df = spark.sql("select f3.*, coalesce(padf.preferential_attachment_score , 0.0 )  as preferential_attachment_score2 from global_temp.f3 as f3 inner join global_temp.padf as padf on f3.authorId2 = padf.vertexid");
    featureset4df.write.mode("overwrite").option("sep", "|").option("header", "true").csv(const.dstFolderPath +const.validationfeaturesetFileName);


    featureset4df.show(30);
    println(featureset4df.count());
  }

}
