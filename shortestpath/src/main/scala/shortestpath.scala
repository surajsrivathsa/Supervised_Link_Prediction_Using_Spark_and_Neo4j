import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.SQLContext
import org.graphframes._
import org.apache.spark.graphx.lib.ShortestPaths
import org.apache.spark.graphx._;
import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._


object shortestpath extends App{
  val spark = SparkSession.builder.master("local[2]").appName("load_graph_data").config("spark.executor.memory", "3g").getOrCreate;
  import spark.implicits._

  val mlschema = StructType(Array(StructField("authorId1", LongType, nullable = false), StructField("authorId2", LongType, nullable = false),
    StructField("paperId", LongType, nullable = false),StructField("linkexists", StringType, nullable = false)))

  var mldf = spark.read.schema(mlschema).format("csv").option("header", "false").option("mode", "DROPMALFORMED").option("inferSchema", "false").load("/Users/surajshashidhar/Desktop/graphml_10lkh/sps.csv");
  mldf.show(10);

  var vertex = Seq(("a1",-1, "author"), ("a2",-1, "author"),("a3",-1, "author"), ("a4",-1, "author"), ("a5",-1, "author"),("a6",-1, "author"),
    ("a7",-1, "author"),("a8",-1, "author"), ("a9",-1, "author"),("a10",-1, "author"),("a11",-1, "author"),("a13",-1, "author"),("a14",-1, "author"))
    .toDF("id", "dummy", "vertex_label");

  var edges = Seq(("a1", "a4", "p1", 1991, "authorship"), ("a4", "a1", "p1", 1991, "authorship"), ("a1", "a4", "p2", 1991, "authorship"),("a4", "a1", "p2", 1991, "authorship"),
    ("a2", "a1", "p2", 1991, "authorship"),("a1", "a2", "p2", 1991, "authorship"),("a1", "a7", "p2", 1991, "authorship"),("a7", "a1", "p2", 1991, "authorship"),
    ("a4", "a2", "p2", 1991, "authorship"),("a2", "a4", "p2", 1991, "authorship"),("a4", "a7", "p2", 1991, "authorship"),("a7", "a4", "p2", 1991, "authorship"),("a2", "a7", "p2", 1991, "authorship"),("a7", "a2", "p2", 1991, "authorship"),
    ("a4", "a9", "p10", 1991, "authorship"),("a9", "a4", "p10", 1991, "authorship"),("a10", "a9", "p10", 1991, "authorship"),("a9", "a10", "p10", 1991, "authorship"),
    ("a4", "a9", "p10", 1991, "authorship"),("a9", "a4", "p10", 1991, "authorship"),("a10", "a9", "p8", 1991, "authorship"),("a9", "a10", "p8", 1991, "authorship"),
    ("a7", "a9", "p8", 1991, "authorship"),("a9", "a7", "p8", 1991, "authorship"),("a7", "a10", "p8", 1991, "authorship"),("a10", "a7", "p8", 1991, "authorship"),
    ("a7", "a8", "p3", 1991, "authorship"),("a8", "a7", "p3", 1991, "authorship"),("a7", "a5", "p3", 1991, "authorship"),("a5", "a7", "p3", 1991, "authorship"),
    ("a8", "a5", "p3", 1991, "authorship"),("a5", "a8", "p3", 1991, "authorship"),("a8", "a6", "p5", 1991, "authorship"),("a6", "a8", "p5", 1991, "authorship"),("a3", "a6", "p6", 1991, "authorship"),("a6", "a3", "p6", 1991, "authorship"),
    ("a3", "a2", "p7", 1991, "authorship"),("a2", "a3", "p7", 1991, "authorship"),("a3", "a8", "p7", 1991, "authorship"),("a8", "a3", "p7", 1991, "authorship"),("a8", "a2", "p7", 1991, "authorship"),("a2", "a8", "p7", 1991, "authorship"),
    ("a10", "a13", "p4", 1991, "authorship"),("a13", "a10", "p4", 1991, "authorship"),("a11", "a14", "p5", 1991, "authorship"),("a14", "a11", "p5", 1991, "authorship")).toDF("src", "dst", "paperid","year","edge_label");


  vertex.cache(); edges.cache();
  val graph = GraphFrame(vertex, edges)

  //graph.vertices.show(10)

  //graph.edges.show(10);

  var gx = graph.toGraphX;

  gx.cache()
  println("-----  the vertices are ------");
  gx.vertices.collect().foreach(println);
  gx.edges.collect().foreach(println);

  val example = new Astar_path(spark);

  //mldf.show(10);
  //mldf.select("authorId1","authorId2").as[(Long, Long)].map{case (a1, a2) => example.single_shortest_path(a1, a2, gx)}

 // mldf.select("authorId1", "authorId2").as[(Long, Long)].map{case (a1, a2) => example.single_shortest_path(a1, a2, gx)}.show(10)
  //p.show(10)

  //mldf.withColumn("newcol", example.single_shortest_path(col("authorId1"), col("authorId2"), gx))
  println("-----  the vertices are ------");
  gx.vertices.collect().foreach(println);
  var exists1 = example.single_shortest_path(1297080123392L,798863917056L,gx);

  var exists2 = example.single_shortest_path(1047972020224L, 1297080123392L, gx);

  var not_exists = example.single_shortest_path(292057776128L, 343597383680L, gx);

  println(" ---- " + exists1 + " --------- " + exists2 + " --------- " + not_exists);

  gx.triplets.map(triplet => triplet.srcAttr(0) + " and " +  triplet.dstAttr(0) + " authored " + triplet.attr(2) + " in the year " + triplet.attr(3)).collect().foreach(println);


  val sourceId = 1297080123392L
  val dstId = 1047972020224L;
  //val g = gx.mapVertices( (id, _) => if (id == sourceId) 0.0 else Double.PositiveInfinity)
  //g.triplets.collect().foreach(println)

  val g = gx.mapVertices( (id, _) => if (id == sourceId) Array(0.0, id) else Array(Double.PositiveInfinity, id)).mapEdges( e => 1 )

  g.triplets.collect().foreach(println)
  //g.vertices.collect().foreach(println)
  g.vertices.map(row => row._1 + " ---- " + row._2(0) + " , " + row._2(1) ).collect().foreach(println)
  val sssp = g.pregel(Array(Double.PositiveInfinity, -1))((id, dist, newDist) => {
      if (dist(0) < newDist(0)) dist
      else newDist},
    triplet => { if (triplet.srcAttr(0).asInstanceOf[Long] + triplet.attr.toString < triplet.dstAttr(0).toString) {
        Iterator((triplet.dstId, Array(triplet.srcAttr(0) + triplet.attr, triplet.srcId)))}
      else {Iterator.empty}},
    (a, b) => {if (a(0) < b(0)) a else b}
  )


  //mldf.select("authorId1","authorId2").as[(Long, Long)].map{case (a1, a2) => example.single_shortest_path(a1, a2, gx)}

  /*
  var sp_all = graph.shortestPaths.landmarks(Seq("a1", "a3", "a11")).run()
  sp_all.select("id", "distances").show()
  sp_all.show(100)
  println("--the count is: " + sp_all.count());

    var gf = graph.toGraphX;

    gf.vertices.take(5).foreach(println)

    val result = ShortestPaths.run(gf, Seq(987842478080))

    val shortestPath = result               // result is a graph
      .vertices                             // we get the vertices RDD
      .filter({case(vId, _) => vId == v1})  // we filter to get only the shortest path from v1
      .first                                // there's only one value
      ._2                                   // the result is a tuple (v1, Map)
      .get(v2)

     */

  /*
  var front_edges = Seq(("p1", "a1", "authorship"), ("p1", "a4", "authorship"),("p2", "a4", "authorship"),("p2", "a1", "authorship"),("p2", "a7", "authorship"),("p2", "a2", "authorship"),("p10", "a4", "authorship"),("p10", "a9", "authorship"),("p8", "a7", "authorship"),("p8", "a9", "authorship"),("p8", "a10", "authorship"),("p9", "a9", "authorship"),("p9", "a10", "authorship"),("p3", "a8", "authorship"),("p3", "a5", "authorship"),("p3", "a7", "authorship"),("p7", "a2", "authorship"),("p7", "a3", "authorship"),("p7", "a6", "authorship"),("p7", "a8", "authorship"),("p6", "a3", "authorship"),("p6", "a6", "authorship"),("p5", "a6", "authorship"),("p5", "a8", "authorship")).toDF("src", "dst", "edge_label")

  var back_edges = front_edges.select("dst", "src", "edge_label")
  back_edges = back_edges.withColumn("edge_label", when(col("edge_label").equalTo("authorship"), "rev"))

  var nodes = Seq(("a1",-1, "author"), ("a2",-1, "author"),("a3",-1, "author"), ("a4",-1, "author"), ("a5",-1, "author"),("a6",-1, "author"), ("a7",-1, "author"),("a8",-1, "author"), ("a9",-1, "author"),("a10",-1, "author"), ("p1",1991, "paper"),("p2",1991, "paper"),("p3",1991, "paper"),("p4",1991, "paper"),("p5",1991, "paper"),("p6",1991, "paper"),("p7",1991, "paper"),("p8",1991, "paper"),("p9",1991, "paper"),("p10",1991, "paper")).toDF("src", "dst", "vertex_label").toDF("id", "year", "vertex_label")
  var edges  =front_edges.union(back_edges)
  edges.show(50);

  val gf = GraphFrame(nodes, edges)
  nodes.persist()
  edges.persist()
  println(nodes.count)
  println(edges.count)
*/




}
