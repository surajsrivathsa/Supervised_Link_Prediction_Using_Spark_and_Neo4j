package pkg

import org.apache.spark.sql.types._


class constantsFile extends Serializable {

  //spark program configs
  var executor_memory = "4g"
  var driver_memory = "2g"
  var master = "local[2]"
  var appName = "calculate_second_shortest_path_app"

  //defining schemas
  val vertexSchema  = StructType(Array(StructField("id", LongType, nullable = false),StructField("label", StringType, nullable = false)))

  val edgesSchema =  StructType(Array(StructField("src", LongType, nullable = false), StructField("dst", LongType, nullable = false),StructField("paperId", LongType, nullable = false),StructField("year", IntegerType, nullable = false),StructField("label", StringType, nullable = false)))

  val authorTupleSchema = StructType(Array(StructField("authorId1", LongType, nullable = false), StructField("authorId2", LongType, nullable = false),StructField("year", IntegerType, nullable = true),StructField("expected_label", IntegerType, nullable = false)))


  val sspData = StructType(Array(StructField("authorId1", LongType, nullable = false), StructField("authorId2", LongType, nullable = false),StructField("first_shortest_path", DoubleType, nullable = true),StructField("second_shortest_path", DoubleType, nullable = false)))

  val sspSample = StructType(Array(StructField("authorId1", LongType, nullable = false), StructField("authorId2", LongType, nullable = false)));


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

  var sspsamplePath = "file:///Users/surajshashidhar/Desktop/dataset_20200127/testing_samples_ssp.csv";
  var sspdataPath = "file:///Users/surajshashidhar/Desktop/dataset_20200127/ssp_testing.csv";


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

  var testingsspFolder = "ssp_testing"
  var trainingsspFolder = "ssp_traning"



/*

  var commonFolderPath = "file:///Users/surajshashidhar/Desktop/graphml/"
  var srcFolderPath = "file:///Users/surajshashidhar/Desktop/graphml/input/"
  var dstFolderPath = "file:///Users/surajshashidhar/Desktop/graphml/output/";

  var vertexFolder = "all_vertices"
  var edgesFolder = "all_edges"

  /*
  var trainingsamplesFolder = "recent_graph_data_for_neo4j"
  var testingsamplesFolder = "recent_graph_data_for_neo4j"
  var trainingsubgraphverticesFolder = "recent_graph_data_for_neo4j"
  var testingsubgraphverticesFolder = "recent_graph_data_for_neo4j"
  var trainingsubgraphedgesFolder = "recent_graph_data_for_neo4j"
  var testingsubgraphedgesFolder = "recent_graph_data_for_neo4j"

   */


  var trainingsamplesFolder = "training_samples"
  var testingsamplesFolder = "testing_samples"
  var trainingsubgraphverticesFolder = "training_subgraph_vertices_neo4j"
  var testingsubgraphverticesFolder = "testing_subgraph_vertices_neo4j"
  var trainingsubgraphedgesFolder = "training_subgraph_edges_neo4j"
  var testingsubgraphedgesFolder = "testing_subgraph_edges_neo4j"


  var trainingfspFolder = "fsp_training"
  var testingfspFolder = "fsp_testing"


 */

}
