package pkg

import org.apache.spark.sql.types._


class constantsFile extends Serializable {

  //spark program configs
  var executor_memory = "4g"
  var driver_memory = "2g"
  var master = "yarn"
  var appName = "calculate_ccf_feature_app"

  //defining schemas
  val vertexSchema  = StructType(Array(StructField("id", LongType, nullable = false),StructField("label", StringType, nullable = false)))

  val edgesSchema =  StructType(Array(StructField("src", LongType, nullable = false), StructField("dst", LongType, nullable = false),StructField("paperId", LongType, nullable = false),StructField("year", IntegerType, nullable = false),StructField("label", StringType, nullable = false)))

  val authorTupleSchema = StructType(Array(StructField("authorId1", LongType, nullable = false), StructField("authorId2", LongType, nullable = false),StructField("year", IntegerType, nullable = true),StructField("expected_label", IntegerType, nullable = false)))

  //defining program constants

  //common variables
  var year1 = 1989
  var year2 = 1990
  var year3 = 1991

  //pagerank file variables
  var year1_filter = "year <= " + year1
  var year2_filter = "year <= " + year2
  var year3_filter = "year <= " + year3

  var convergence_iteration = 40;
  var maxcomponentsize = 1000000
  var maxcomponentrecords = 10000000




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


  var trainingfspFolder = "fsp_training"
  var testingfspFolder = "fsp_testing"

  var flatteneddfFolder = "flattened_df"

  var ccftrainingFolder = "ccfeature_training"
  var ccftestingFolder = "ccfeature_testing"
  var connectedcomponentstrainingFolder = "connected_components_training"
  var connectedcomponentstestingFolder = "connected_components_testing"

  var flattenedtrainingdfFolder = "flattened_training_df"
  var flattenedtestingdfFolder = "flattened_testing_df"

/*
  var commonFolderPath = "file:///Users/surajshashidhar/Desktop/graphml/"
  var srcFolderPath = "file:///Users/surajshashidhar/Desktop/graphml/input/"
  var dstFolderPath = "file:///Users/surajshashidhar/Desktop/graphml/output/";

  var vertexFolder = "all_vertices"
  var edgesFolder = "all_edges"

  var trainingsamplesFolder = "training_samples"
  var testingsamplesFolder = "testing_samples"
  var trainingsubgraphverticesFolder = "training_subgraph_vertices"
  var testingsubgraphverticesFolder = "testing_subgraph_vertices"
  var trainingsubgraphedgesFolder = "training_subgraph_edges"
  var testingsubgraphedgesFolder = "testing_subgraph_edges"

  var trainingfspFolder = "fsp_training"
  var testingfspFolder = "fsp_testing"

    var flatteneddfFolder = "flattened_df"


 */

}
