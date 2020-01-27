package pkg

import org.apache.spark.sql.types._

class Constants {

  // job settings
  var appName = "Preprocessing_App"
  //var master = "local[*]"
  var master = "yarn"//change to "local[*]" while running from local

  //schemas
  val paperSchema = StructType(Array(StructField("paperId", LongType, nullable = false), StructField("paperLabel", StringType, nullable = false)))

  val authorSchema = StructType(Array(StructField("authorId", LongType, nullable = false), StructField("authorLabel", StringType, nullable = false)))

  val authorshipSchema = StructType(Array(StructField("paperId", LongType, nullable = false), StructField("authorId", LongType, nullable = false)));


  //comment the below hdfs variables and uncomment local file system paths while running in local

  //application based constants


  //local path constants
//  var author_vertices_input_path = "file:///Users/surajshashidhar/Desktop/graphml/input/author_vertices/*"
//  var paper_vertices_input_path = "file:///Users/surajshashidhar/Desktop/graphml/input/paper_vertices/*"
//  var authorship_edges_input_path = "file:///Users/surajshashidhar/Desktop/graphml/input/authorship_edges/*"
//  var coauthorship_edges_input_path = "file:///Users/surajshashidhar/Desktop/graphml/input/coauthorship_edges/*"
//
//  var author_vertices_output_path = "file:///Users/surajshashidhar/Desktop/graphml/input/preprocessed_author_vertices"
//  var paper_vertices_output_path = "file:///Users/surajshashidhar/Desktop/graphml/input/preprocessed_paper_vertices"
//  var authorship_edges_output_path = "file:///Users/surajshashidhar/Desktop/graphml/input/preprocessed_authorship_edges"
//  var coauthorship_edges_output_path = "file:///Users/surajshashidhar/Desktop/graphml/input/preprocessed_coauthorship_edges"



  //hdfs path constants

  var author_vertices_input_path = "hdfs:///user/avanish1/input/author_vertices/*"
  var paper_vertices_input_path = "hdfs:///user/avanish1/input/paper_vertices/*"
  var authorship_edges_input_path = "hdfs:///user/avanish1/input/authorship_edges/*"
  var coauthorship_edges_input_path = "hdfs:///user/avanish1/input/coauthorship_edges/*"

  var author_vertices_output_path = "hdfs:///user/avanish1/input/preprocessed_author_vertices"
  var paper_vertices_output_path = "hdfs:///user/avanish1/input/preprocessed_paper_vertices"
  var authorship_edges_output_path = "hdfs:///user/avanish1/input/preprocessed_authorship_edges"
  var coauthorship_edges_output_path = "hdfs:///user/avanish1/input/preprocessed_coauthorship_edges"
  var author_author_edges_output_path = "hdfs:///user/avanish1/input/preprocessed_author_author_edges"

}





