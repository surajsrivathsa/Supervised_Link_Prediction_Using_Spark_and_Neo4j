package pkg

import org.apache.spark.sql.types._


class constantsFile extends Serializable {

  //spark program configs
  var executor_memory = "4g"
  var driver_memory = "2g"
  var master = "yarn"
  var appName = "connectedcomponents_app"

  //defining schemas
  val vertexSchema  = StructType(Array(StructField("id", LongType, nullable = false),StructField("label", StringType, nullable = false)))

  val edgesSchema =  StructType(Array(StructField("src", LongType, nullable = false), StructField("dst", LongType, nullable = false),StructField("paperId", LongType, nullable = false),StructField("year", IntegerType, nullable = false),StructField("label", StringType, nullable = false)))

  //defining program constants

  // connected components variables
  val convergence_iteration = 100
  val maxcomponentsize = 200000
  var maxcomponentrecords = 200000
  var maxcomponentrecords1 = 1000000

  //defining path


  var commonFolderPath = "hdfs:///user/avanish1/input/"
  var srcFolderPath = "hdfs:///user/avanish1/input/"
  var dstFolderPath = "hdfs:///user/avanish1/output/";

  var vertexFolder = "preprocessed_author_vertices"
  var edgesFolder = "preprocessed_author_author_edges"
  var connectedcomponentsFolder = "new_connected_components"
  var subgraphvertexFolder = "subgraph_vertices"
  var subgraphedgesFolder = "subgraph_edges"
  var flatteneddfFolder = "flattened_df"


/*
  var commonFolderPath = "file:///Users/surajshashidhar/Desktop/graphml/"
  var srcFolderPath = "file:///Users/surajshashidhar/Desktop/graphml/input/"
  var dstFolderPath = "file:///Users/surajshashidhar/Desktop/graphml/output/";

  var vertexFolder = "all_vertices"
  var edgesFolder = "all_edges"
  var connectedcomponentsFolder = "connected_components"
  var subgraphvertexFolder = "subgraph_vertices"
  var subgraphedgesFolder = "subgraph_edges"
  var flatteneddfFolder = "flattened_df"



 */

}
