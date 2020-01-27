package pkg

import org.apache.spark.sql.types._


class constantsFile extends Serializable {

  //spark program configs
  var executor_memory = "4g"
  var driver_memory = "2g"
  var master = "yarn"
  var appName = "assemble_features_app"

  //defining schemas
  val vertexSchema  = StructType(Array(StructField("id", LongType, nullable = false),StructField("label", StringType, nullable = false)))

  val edgesSchema =  StructType(Array(StructField("src", LongType, nullable = false), StructField("dst", LongType, nullable = false),StructField("paperId", LongType, nullable = false),StructField("year", IntegerType, nullable = false),StructField("label", StringType, nullable = false)))

  val authorTupleSchema = StructType(Array(StructField("authorId1", LongType, nullable = false), StructField("authorId2", LongType, nullable = false),StructField("year", IntegerType, nullable = true),StructField("expected_label", IntegerType, nullable = false)))

  val firstshortestpathSchema = StructType(Array(StructField("src", LongType, nullable = false), StructField("dst", LongType, nullable = false), StructField("distance", DoubleType, nullable = false)))

  var pagerankSchema = StructType(Array(StructField("vertexid", LongType, nullable = false), StructField("pagerankscore", DoubleType, nullable = false)))

  val preferentialattachmentSchema = StructType(Array(StructField("preferential_attachment_score", DoubleType, nullable = false), StructField("numberofconnectednodes", IntegerType, nullable = false),StructField("vertexid", LongType, nullable = false),StructField("year", IntegerType, nullable = true)))

  val commonneighboursSchema = StructType(Array(StructField("authorId1", LongType, nullable = false), StructField("authorId2", LongType, nullable = false), StructField("common_neighbours", IntegerType, nullable = false)))

  val secondshortestpathSchema = StructType(Array(StructField("src", LongType, nullable = false), StructField("dst", LongType, nullable = false), StructField("distance1", DoubleType, nullable = false), StructField("distance2", DoubleType, nullable = false)))


  //defining program constants

  //common variables
  var year1 = 2016
  var year2 = 2017
  var year3 = 2018

  //pagerank file variables
  var year1_filter = "year <= " + year1
  var year2_filter = "year <= " + year2
  var year3_filter = "year <= " + year3


  //defining path


  var commonFolderPath = "hdfs:///user/avanish1/input/"
  var srcFolderPath = "hdfs:///user/avanish1/input/"
  var dstFolderPath = "hdfs:///user/avanish1/output/";

  var vertexFolder = "all_vertices"
  var edgesFolder = "all_edges"

  var trainingsamplesFolder = "training_samples"
  var testingsamplesFolder = "testing_samples"
  var trainingsubgraphverticesFolder = "training_subgraph_vertices"
  var testingsubgraphverticesFolder = "testing_subgraph_vertices"
  var trainingsubgraphedgesFolder = "training_subgraph_edges"
  var testingsubgraphedgesFolder = "testing_subgraph_edges"

    var trainingpagerankFolder = "pagerank_training"
    var testingpagerankFolder = "pagerank_testing"
    var trainingprefattachFolder = "preferential_attachment_training"
    var testingprefattachFolder = "preferential_attachment_testing"
    var trainingcnFolder = "common_neighbours_training"
    var testingcnFolder = "common_neighbours_testing"
    var trainingfspFolder = "fsp_training"
    var testingfspFolder = "fsp_testing"
  var trainingsspFolder = "ssp_traning"
  var testingsspFolder = "ssp_testing"

    var trainingfeaturesFolder = "assembled_features_training"
    var testingfeaturesFolder = "assembled_features_testing"

  //var trainingdynamicpagerankFolder = "dynamic_pagerank_training"
  //var testingdynamicpagerankFolder = "dynamic_pagerank_testing"

  var trainingglobalprefattachFolder = "global_preferential_attachment_training"
  var testingglobalprefattachFolder = "global_preferential_attachment_testing"
  var trainingglobalpagerankFolder = "global_pagerank_training"
  var testingglobalpagerankFolder = "global_pagerank_testing"




/*
      var commonFolderPath = "file:///Users/surajshashidhar/Desktop/graphml/"
      var srcFolderPath = "file:///Users/surajshashidhar/Desktop/graphml/input/"
      var dstFolderPath = "file:///Users/surajshashidhar/Desktop/graphml/new_dataset_1/";

      var vertexFolder = "all_vertices"
      var edgesFolder = "all_edges"

      var trainingsamplesFolder = "training_samples"
      var testingsamplesFolder = "testing_samples"
      var trainingsubgraphverticesFolder = "training_subgraph_vertices"
      var testingsubgraphverticesFolder = "testing_subgraph_vertices"
      var trainingsubgraphedgesFolder = "training_subgraph_edges"
      var testingsubgraphedgesFolder = "testing_subgraph_edges"

      var trainingpagerankFolder = "pagerank_training"
      var testingpagerankFolder = "pagerank_testing"
      var trainingprefattachFolder = "preferential_attachment_training"
      var testingprefattachFolder = "preferential_attachment_testing"
      var trainingcnFolder = "common_neighbours_training"
      var testingcnFolder = "common_neighbours_testing"
      var trainingfspFolder = "fsp_training"
      var testingfspFolder = "fsp_testing"

      var trainingfeaturesFolder = "assmebled_features_training"
      var testingfeaturesFolder = "assembled_features_testing"

      //var trainingdynamicpagerankFolder = "dynamic_pagerank_training"
      //var testingdynamicpagerankFolder = "dynamic_pagerank_testing"

      var trainingglobalprefattachFolder = "global_preferential_attachment_training"
      var testingglobalprefattachFolder = "global_preferential_attachment_testing"
  var trainingglobalpagerankFolder = "global_pagerank_training"
  var testingglobalpagerankFolder = "global_pagerank_testing"

 */




}
