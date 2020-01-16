package pkg

import java.math.BigDecimal
import org.apache.spark.graphx._
import org.apache.spark.sql._
import org.graphframes._

class precompute_PreferentialAttachment (spark: SparkSession) {

  def preferentialAttachment(graph: GraphFrame, const: constantsFile) : Unit =
  {

    var year1 = const.year1; var year2 = const.year2; var year3 = const.year3;
  println("------ " + year1 + " : " + year2 + " : " + year3 + " ------------")


    val graph_year1 = graph.filterEdges(const.year1_filter);
    val graph_year2 = graph.filterEdges(const.year2_filter);
    //val graph_year3 = graph.filterEdges(const.year3_filter);
    val gx_year1 = graph_year1.toGraphX;
    val gx_year2 = graph_year2.toGraphX;
    //val gx_year3 = graph_year3.toGraphX;

    gx_year1.persist(); gx_year2.persist(); //gx_year3.persist()

    val neighbours_year1 = gx_year1.collectNeighborIds(EdgeDirection.Either);
    val neighbours_year2 = gx_year2.collectNeighborIds(EdgeDirection.Either);
    //val neighbours_year3 = gx_year3.collectNeighborIds(EdgeDirection.Either);
    val totalEdges_year1 = gx_year1.edges.count();
    val totalEdges_year2 = gx_year2.edges.count();
    //val totalEdges_year3 = gx_year3.edges.count();

    println("   -----------------------  "+ "the count of edges for the three years are: " + totalEdges_year1 + " ---- " + totalEdges_year2 + " ---- "+ -1 + " ---- ");
    var pref_attachment_year1 = neighbours_year1.map(line => (line._2.length.toDouble/(2.0*totalEdges_year1),line._2.length,line._1));
    var pref_attachment_year2 = neighbours_year2.map(line => (line._2.length.toDouble/(2.0*totalEdges_year2),line._2.length,line._1));
    //var pref_attachment_year3 = neighbours_year3.map(line => (line._2.length.toDouble/(2.0*totalEdges_year3),line._2.length,line._1));
    //pref_attachment_1989.sortBy(line => line._1,false).take(20).foreach(println);
    println("     --------------------------     ")
    //pref_attachment_1990.sortBy(line => line._1,false).take(20).foreach(println);
    println("     --------------------------     ")
    //pref_attachment_1991.sortBy(line => line._1,false).take(20).foreach(println);

    pref_attachment_year1.map(line => new BigDecimal(line._1).toPlainString() + "|" + line._2 + "|" + line._3 + "|" + year1).saveAsTextFile(const.dstFolderPath + const.prefattachFileName + "training");
    pref_attachment_year2.map(line => new BigDecimal(line._1).toPlainString() + "|" + line._2 + "|" + line._3 + "|" + year2).saveAsTextFile(const.dstFolderPath + const.prefattachFileName + "testing");
    //pref_attachment_year3.map(line => new BigDecimal(line._1).toPlainString() + "|" + line._2 + "|" + line._3 + "|" + year3).saveAsTextFile(const.dstFolderPath + const.prefattachFileName + const.year3);


  }
  /*
  // Import random graph generation library
  // Create a graph with "age" as the vertex property.  Here we use a random graph for simplicity.
  val graph: Graph[Double, Int] =
    GraphGenerators.logNormalGraph(spark.sparkContext, numVertices = 100).mapVertices( (id, _) => id.toDouble )

  graph.triplets.collect().foreach(println);
  graph.edges.take(10).foreach(println);
  graph.vertices.take(10).foreach(println);
  // Compute the number of older followers and their total age
  val olderFollowers: VertexRDD[(Int, Double)] = graph.aggregateMessages[(Int, Double)](
    triplet => { // Map Function
      if (triplet.srcAttr < triplet.dstAttr) {
        // Send message to destination vertex containing counter and age
        triplet.sendToDst(1, triplet.srcAttr)
      }
    },
    // Add counter and age
    (a, b) => (a._1 + b._1, a._2 + b._2) // Reduce Function
  )
  // Divide total age by number of older followers to get average age of older followers
  val avgAgeOfOlderFollowers: VertexRDD[Double] =
    olderFollowers.mapValues( (id, value) => value match { case (count, totalAge) => totalAge / count } )
  // Display the results
  avgAgeOfOlderFollowers.collect.foreach(println(_))
}

   */

}
