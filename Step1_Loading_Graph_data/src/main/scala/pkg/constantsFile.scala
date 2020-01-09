package pkg

import org.apache.spark.sql.types._


class constantsFile {

  //spark program configs
  var executor_memory = "4g"
  var driver_memory = "2g"
  var master = "yarn"
  var appName = "LoadGraph"

  //defining schemas
  val paperSchema = StructType(Array(StructField("paperId", LongType, nullable = false), StructField("year", IntegerType, nullable = false) , StructField("paperLabel", StringType, nullable = false)))

  val authorSchema = StructType(Array(StructField("id", LongType, nullable = false),StructField("label", StringType, nullable = false)))

  val authorshipSchema = StructType(Array(StructField("paperId", LongType, nullable = false), StructField("authorId", LongType, nullable = false), StructField("Label", StringType, nullable = false)))

  val mlschema = StructType(Array(StructField("authorId1", LongType, nullable = false), StructField("authorId2", LongType, nullable = false), StructField("label", StringType, nullable = false),StructField("common_neighbours", DoubleType, nullable = false),StructField("pagerankscore1", DoubleType, nullable = false), StructField("pagerankscore2", DoubleType, nullable = false), StructField("preferential_attachment_score1", DoubleType, nullable = false), StructField("preferential_attachment_score2", DoubleType, nullable = false)))

  val pagerankSchema = StructType(Array(StructField("authorId", LongType, nullable = false), StructField("pagerank_value", DoubleType, nullable = false)))

  //defining program constants

  //common variables
  var year1 = 1989
  var year2 = 1990
  var year3 = 1991

  //pagerank file variables
  var year1_filter = "year <= " + year1
  var year2_filter = "year <= " + year2
  var year3_filter = "year <= " + year3

  var pagerank_probability = 0.15
  var pagerank_tolerance = 0.01
  var pagerank_iteration = 10

  //preferential attachment variables

  //2nd step variables
  var step2_recordlimit = 7000

  var s2f_year1_filter = "year == " + year1
  var s2f_year2_filter = "year == " + year2
  var s2f_year3_filter = "year == " + year3

  var s2f_year_filter = "year == " + year2
  var s2f_year = year2

  var notexistscount = 6000
  var existscount = 100000

  //common neighbours variables


  //defining path


  var commonFolderPath = "hdfs:///user/avanish1/input/"
  var srcFolderPath = "hdfs:///user/avanish1/input/"
  var dstFolderPath = "hdfs:///user/avanish1/output/";

  var authorFileName = "Authors.csv"
  var paperFileName = "Papers.csv"
  var authorshipFileName = "Authorship.csv"
  var samples_trainingFileName = "samples_1990_training.csv"
  var trainfeaturesetFileName = "training_features"
  var validationfeaturesetFileName = "validation_features"
  var pagerankFileName = "pagerank_"
  var prefattachFileName = "prefrential_attachment_"
  var s2f_training_FileName = "samples_training"
  var commonneighbourstmpFileName = "tmp_cn_"
  var commonneighboursFileName = "common_neighbours_"
  var s2f_validation_FileName = "samples_validation"


/*
  var commonFolderPath = "file:///Users/surajshashidhar/Desktop/graphml_10lkh/"
  var srcFolderPath = "file:///Users/surajshashidhar/Desktop/graphml_10lkh/"
  var dstFolderPath = "file:///Users/surajshashidhar/Desktop/graphml_10lkh/";

  var authorFileName = "Authors_10lkh.csv"
  var paperFileName = "Papers_10lkh.csv"
  var authorshipFileName = "Authorship_10lkh.csv"
  var samples_trainingFileName = "samples_1990_training.csv"
  var trainfeaturesetFileName = "training_features"
  var validationfeaturesetFileName = "validation_features"
  var pagerankFileName = "pagerank_"
  var prefattachFileName = "prefrential_attachment_"
  var s2f_training_FileName = "samples_training"
  var commonneighbourstmpFileName = "tmp_cn_"
  var commonneighboursFileName = "common_neighbours_"
  var s2f_validation_FileName = "samples_validation"



 */



}
