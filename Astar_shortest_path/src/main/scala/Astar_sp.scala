
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.graphx._
import org.apache.spark.graphx.lib.ShortestPaths
import org.apache.spark.sql.{SparkSession, _}
import org.apache.spark.sql.functions._
import graphframes._
import org.apache.spark.graphx
import org.graphframes.GraphFrame
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

class Astar_sp (spark: SparkSession){

  def calculate_astar_sp(graph: Graph[Row, Row], srcId: graphx.VertexId, tgtId: graphx.VertexId): Array[Double] = {

    val gx = graph.mapVertices( (id, _) =>
      if (id == srcId) Array(0.0,0.0, id, id)
      else Array(Double.PositiveInfinity,Double.PositiveInfinity, id, id)).
      mapEdges( e => 1 )

    val msssp = gx.pregel(Array(Double.PositiveInfinity,  Double.PositiveInfinity, -1, -1))(
      (id, dist, newDist) =>
      {
        if (dist(0) <= newDist(0) && dist(1) <= newDist(1))
        {
          //println(id);
          dist
        }
        else if(dist(0) <= newDist(0) && dist(1) > newDist(1))
        {
          //println(dist.mkString(" ---dist--- ")+ " --- "+ newDist.mkString(" newDist "))
          Array(dist(0), newDist(1), dist(2))
        }
        else if(dist(0) > newDist(0) && dist(1) <= newDist(1) )
        {
          Array(newDist(0), dist(1), newDist(2))
        }
        else if(dist(0) > newDist(0) && dist(1) > newDist(1))
        {
          newDist
        }
        else {
          newDist
        }
      },

      triplet => {
        //println(triplet.srcId + " | " + triplet.srcAttr(0) + " | " + triplet.srcAttr(1) + " | " + triplet.attr + " | "+ triplet.dstAttr(0) + " | "+ triplet.dstAttr(1) + " | " + triplet.dstId)

        if(triplet.srcId == srcId)
        {
          if(triplet.srcAttr(0) + triplet.attr  < triplet.dstAttr(0))
            Iterator((triplet.dstId, Array(triplet.srcAttr(0) + triplet.attr, Double.PositiveInfinity, triplet.srcId)))
          else
            Iterator.empty
        }

        else if (triplet.srcAttr(0) + triplet.attr  < triplet.dstAttr(0) && triplet.srcAttr(1) + triplet.attr  < triplet.dstAttr(1)) {
          //println(triplet.srcId + " |@| " + triplet.srcAttr(0) +  " |@| " + triplet.srcAttr(1) + " |@| " + triplet.attr + " |@| "+ triplet.dstAttr(0) + " |@| " + triplet.dstAttr(1) + " |@| " + triplet.dstId)
          Iterator((triplet.dstId, Array(triplet.srcAttr(0) + triplet.attr, triplet.srcAttr(1) + triplet.attr, triplet.srcId)))
        }
        else if (triplet.srcAttr(0) + triplet.attr  < triplet.dstAttr(0) && triplet.srcAttr(1) + triplet.attr  <= triplet.dstAttr(1)) {
          //println(triplet.srcId + " |@| " + triplet.srcAttr(0) +  " |@| " + triplet.srcAttr(1) + " |@| " + triplet.attr + " |@| "+ triplet.dstAttr(0) + " |@| " + triplet.dstAttr(1) + " |@| " + triplet.dstId)
          Iterator((triplet.dstId, Array(triplet.srcAttr(0) + triplet.attr, triplet.dstAttr(1), triplet.srcId)))
        }
        else if (triplet.srcAttr(0) + triplet.attr  < triplet.dstAttr(0) && triplet.srcAttr(1) + triplet.attr  > triplet.dstAttr(0)) {
          //println(triplet.srcId + " |#| " + triplet.srcAttr(0) +  " |#| " + triplet.srcAttr(1) + " |#| " + triplet.attr + " |#| "+ triplet.dstAttr(0) + " |#| " + triplet.dstAttr(1) + " |#| " + triplet.dstId)
          Iterator((triplet.dstId, Array(triplet.srcAttr(0) + triplet.attr, triplet.dstAttr(0), triplet.srcId)))
        }
        else if (triplet.srcAttr(0) + triplet.attr   > triplet.dstAttr(0) && triplet.srcAttr(0) + triplet.attr  < triplet.dstAttr(1)) {
          //println(triplet.srcId + " |$| " + triplet.srcAttr(0) +  " |$| " + triplet.srcAttr(1) + " |$| " + triplet.attr + " |$| "+ triplet.dstAttr(0) + " |$| " + triplet.dstAttr(1) + " |$| " + triplet.dstId)
          Iterator((triplet.dstId, Array(triplet.dstAttr(0), triplet.srcAttr(0) + triplet.attr, triplet.srcId)))
        }
        else {
          Iterator.empty
        }

      },

      (a, b) => {
        //println("source Id is: " + srcId)
        //println(a.mkString(" ---a--- "))
        //println(b.mkString(" ---b--- "))
        if (a(0) < b(0) && a(1) < b(1))
        { a }
        else if (a(0) < b(0) && a(1) >= b(1))
        { Array(a(0), b(1), a(2), b(3)) }
        else if (a(0) >= b(0) && a(1) < b(1))
        { Array(b(0), a(1), b(2), a(3)) }
        else if (a(0) >= b(0) && a(1) >= b(1))
        { b }
        else
        { b }
      }
    )

    println(" ------  mssp output -------- ");
    //val ans = msssp.vertices.map(vertex => "Target Vertex " + vertex._1 + ": distance is " + vertex._2(0) + ", previous node is Vertex " + vertex._2(2) + " second shortest distance is " + " - " + vertex._2(1) + " , " + vertex._2.length)
    //ans.collect().foreach(println)
    println(" ----------------- Printing the final answer ----------------- ")
    val ans2 = msssp.vertices.map(vertex => "Shortest distances from Source Vertex " + srcId + " to target landmark vertex: " + vertex._1 + " are  " + vertex._2(0) + " | " + vertex._2(1))
    //ans2.take(10).foreach(println)


    val q = msssp.vertices.filter(vertex => vertex._1 == tgtId)
    val z = q.take(5)
    println("the length of z is: " + z.length + " for source and dst id: " + srcId + " , " + tgtId)
    val f = Array(srcId , tgtId, z(0)._2(0), z(0)._2(1))

    //val dstvertex = z(0)
    //val firstshortestpath = z(1)
    //val secondshortestpath = z(1)
    //z(1)
    //val q = msssp.vertices.filter(vertex => vertex._1 == tgtId).map( row => "the distances are : " + row._2(0) + " and " + row._2(1));
    //q.take(10).foreach(println)
    //println(" -- result is ------")
    //q.take(10).foreach(println)
    return f;
  }

}
