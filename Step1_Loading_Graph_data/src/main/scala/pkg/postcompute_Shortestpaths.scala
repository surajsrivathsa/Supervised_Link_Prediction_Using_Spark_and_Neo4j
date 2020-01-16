package pkg

import org.apache.spark.graphx.VertexId
import org.apache.spark
import org.apache.spark.sql
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.graphx._
import org.apache.spark.graphx.lib.ShortestPaths
import org.apache.spark.sql.{SparkSession, _}
import org.apache.spark.graphx.PartitionStrategy.EdgePartition2D
import org.graphframes.GraphFrame
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.Set

class postcompute_Shortestpaths(spark: SparkSession) {

  def calc_shortestpaths(const: constantsFile, trainingGraph: GraphFrame, testingGraph: GraphFrame,  trainingSamples: Dataset[Row], testingSamples: Dataset[Row]): Unit =
  {
    /*
    val trainingGraph_gx = trainingGraph.toGraphX.partitionBy(EdgePartition2D)
    val testingGraph_gx = testingGraph.toGraphX.partitionBy(EdgePartition2D)


    trainingGraph_gx.persist();
    testingGraph_gx.persist();

    var myvalues = Seq((2102027032L,2170930975L),(110693L,311264L),(300971L, 21057L),(260150L,110693L),(110693L,89L),(1986696157L,4585803L),(250125303L, 523391756L), (385484088L, 2643747584L), (385484088L, 0L), (385484088L, 2643747584L), (385484088L, 2643747584L))

    println("Count of converted graphs edges are: " + trainingGraph_gx.edges.count() + " | " + testingGraph_gx.edges.count())

    val interim1 = trainingSamples.rdd.map(row => (row(0).asInstanceOf[VertexId], row(1).asInstanceOf[VertexId])).collect().toSeq.take(70)
    val interim2 = testingSamples.rdd.map(row => (row(0).asInstanceOf[VertexId], row(1).asInstanceOf[VertexId])).collect().toSeq.take(10)
    println(interim2.mkString(", "))
    val trainingSamples_gx = spark.sparkContext.parallelize(interim1)
    val testingSamples_gx = spark.sparkContext.parallelize(interim2);
    trainingSamples_gx.persist(); testingSamples_gx.persist();

    var trainingDistancesList = new ListBuffer[(VertexId, VertexId, Double, Double)]()
    var testingDistancesList = new ListBuffer[(VertexId, VertexId, Double, Double)]()

     */

/*
    def findShortesPathsinTrainingGraph(srcId: VertexId, tgtId: VertexId): (VertexId, VertexId, Double, Double) =
    {

      val traingx = trainingGraph_gx.mapVertices( (id, _) =>
        if (id == srcId) Array(0.0,0.0, id, id)
        else Array(Double.PositiveInfinity,Double.PositiveInfinity, id, id)).mapEdges( e => 1 )

      val train_msssp = traingx.pregel(Array(Double.PositiveInfinity,  Double.PositiveInfinity, -1, -1))(
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

      val q = train_msssp.vertices.filter(vertex => vertex._1 == tgtId).take(1)
      return (srcId , tgtId, q(0)._2(0).asInstanceOf[Double], q(0)._2(1).asInstanceOf[Double])

    }

    for( authors <- interim1){
      var srcId = authors._1; var tgtId = authors._2
      var tmp = findShortesPathsinTrainingGraph(srcId, tgtId);
      println("Distances array is like : " + tmp.toString())
      trainingDistancesList += tmp
    }

    var trainingResult = spark.sparkContext.parallelize(trainingDistancesList).map(row => row._1 + "|" + row._2 + "|" + row._3 + "|" + row._4)
    trainingResult.saveAsTextFile(const.dstFolderPath + const.shortestpathsFileName + "training");




    def findShortesPathsinTestingGraph(srcId: VertexId, tgtId: VertexId): (VertexId, VertexId, Double, Double) =
    {

      val tstgx = testingGraph_gx.mapVertices( (id, _) =>
        if (id == srcId) Array(0.0,0.0, id, id)
        else Array(Double.PositiveInfinity,Double.PositiveInfinity, id, id)).mapEdges( e => 1 )

      val tst_msssp = tstgx.pregel(Array(Double.PositiveInfinity,  Double.PositiveInfinity, -1, -1))(
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

      val r = tst_msssp.vertices.filter(vertex => vertex._1 == tgtId).take(2)
      //println("r: " +  r.length +  "|"+ r )
      if(r.length == 0)
        return (srcId , tgtId, Double.PositiveInfinity, Double.PositiveInfinity)
      else
        return (srcId , tgtId, r(0)._2(0).asInstanceOf[Double], r(0)._2(1).asInstanceOf[Double])

    }


    for( authors <- interim2){
      var srcId = authors._1; var tgtId = authors._2
      var tmp = findShortesPathsinTestingGraph(srcId, tgtId);
      println("Distances array is like : " + tmp.toString())
      testingDistancesList += tmp
    }

    var testingResult = spark.sparkContext.parallelize(testingDistancesList).map(row => row._1 + "|" + row._2 + "|" + row._3 + "|" + row._4)

    testingResult.saveAsTextFile(const.dstFolderPath + const.shortestpathsFileName + "testing");



    val visitedVertices: Set[VertexId] = Set.empty[VertexId]

    def findShortesPathsinMyGraph(srcId: VertexId, tgtId: VertexId): (VertexId, VertexId, Double, Double) =
    {

      val tstgx = trainingGraph_gx.mapVertices( (id, _) =>
        if (id == srcId) Array(0.0,0.0, mutable.Set.empty[String], 0,id, id)
        else Array(Double.PositiveInfinity,Double.PositiveInfinity, mutable.Set.empty[String], 0,id , id)).mapEdges( e => 1 )



      val tst_msssp = tstgx.pregel(Array(Double.PositiveInfinity,  Double.PositiveInfinity, mutable.Set.empty[String], 0, -1, -1), 5,EdgeDirection.Out)(

        (id, dist, newDist) =>
        {
          if (dist(0).asInstanceOf[Double] <= newDist(0).asInstanceOf[Double] && dist(1).asInstanceOf[Double] <= newDist(1).asInstanceOf[Double])
          {
            dist
          }
          else if(dist(0).asInstanceOf[Double] <= newDist(0).asInstanceOf[Double] && dist(1).asInstanceOf[Double] > newDist(1).asInstanceOf[Double])
          {
            //println(dist.mkString(" ---dist--- ")+ " --- "+ newDist.mkString(" newDist "))
            Array(dist(0), newDist(1), dist(2), dist(3), dist(4))
          }
          else if(dist(0).asInstanceOf[Double] > newDist(0).asInstanceOf[Double] && dist(1).asInstanceOf[Double] <= newDist(1).asInstanceOf[Double] )
          {
            Array(newDist(0), dist(1), newDist(2), dist(3), dist(4))
          }
          else if(dist(0).asInstanceOf[Double] > newDist(0).asInstanceOf[Double] && dist(1).asInstanceOf[Double] > newDist(1).asInstanceOf[Double])
          {
            newDist
          }
          else {
            newDist
          }
        },

        triplet => {
          //println(triplet.srcId + " | " + triplet.srcAttr(0) + " | " + triplet.srcAttr(1) + " | " + triplet.srcAttr(2) + " | " + triplet.attr + " | "+ triplet.dstAttr(0) + " | "+ triplet.dstAttr(1) + " | " +  triplet.dstAttr(2) + " | " + triplet.dstId)
          if(triplet.srcAttr(2).asInstanceOf[Set[String]](triplet.dstId.toString) == false || triplet.srcAttr(3).asInstanceOf[Integer] < 11) {
            //println("adding the following dst vertices to list: " + triplet.dstId.toString)
            //println("added - " + triplet.srcId + " | " + triplet.srcAttr(0) + " | " + triplet.srcAttr(1) + " | " + triplet.srcAttr(2) + " | " + triplet.attr + " | "+ triplet.dstAttr(0) + " | "+ triplet.dstAttr(1) + " | " +  triplet.dstAttr(2) + " | " + triplet.dstId)

            triplet.srcAttr(2).asInstanceOf[Set[String]] += triplet.dstId.toString
            var hopcount = triplet.srcAttr(3).asInstanceOf[Int] + 1;
            if(triplet.srcId == srcId)
            {
              if(triplet.srcAttr(0).asInstanceOf[Double] + triplet.attr.asInstanceOf[Double]  < triplet.dstAttr(0).asInstanceOf[Double])
                Iterator((triplet.dstId, Array(triplet.srcAttr(0).asInstanceOf[Double] + triplet.attr.asInstanceOf[Double], Double.PositiveInfinity, triplet.srcAttr(2), hopcount, triplet.srcId)))
              else
                Iterator.empty
            }
            else if (triplet.srcAttr(0).asInstanceOf[Double] + triplet.attr.asInstanceOf[Double]  < triplet.dstAttr(0).asInstanceOf[Double] && triplet.srcAttr(1).asInstanceOf[Double] + triplet.attr.asInstanceOf[Double]  < triplet.dstAttr(1).asInstanceOf[Double]) {
              //println(triplet.srcId + " |@| " + triplet.srcAttr(0) +  " |@| " + triplet.srcAttr(1) + " |@| " + triplet.attr + " |@| "+ triplet.dstAttr(0) + " |@| " + triplet.dstAttr(1) + " |@| " + triplet.dstId)
              Iterator((triplet.dstId, Array(triplet.srcAttr(0).asInstanceOf[Double] + triplet.attr.asInstanceOf[Double], triplet.srcAttr(1).asInstanceOf[Double] + triplet.attr.asInstanceOf[Double], triplet.srcAttr(2), hopcount, triplet.srcId)))
            }
            else if (triplet.srcAttr(0).asInstanceOf[Double] + triplet.attr.asInstanceOf[Double]  < triplet.dstAttr(0).asInstanceOf[Double] && triplet.srcAttr(1).asInstanceOf[Double] + triplet.attr.asInstanceOf[Double]  <= triplet.dstAttr(1).asInstanceOf[Double]) {
              //println(triplet.srcId + " |@| " + triplet.srcAttr(0) +  " |@| " + triplet.srcAttr(1) + " |@| " + triplet.attr + " |@| "+ triplet.dstAttr(0) + " |@| " + triplet.dstAttr(1) + " |@| " + triplet.dstId)
              Iterator((triplet.dstId, Array(triplet.srcAttr(0).asInstanceOf[Double] + triplet.attr.asInstanceOf[Double], triplet.dstAttr(1).asInstanceOf[Double], triplet.srcAttr(2) , hopcount, triplet.srcId)))
            }
            else if (triplet.srcAttr(0).asInstanceOf[Double] + triplet.attr.asInstanceOf[Double]  < triplet.dstAttr(0).asInstanceOf[Double] && triplet.srcAttr(1).asInstanceOf[Double] + triplet.attr.asInstanceOf[Double]  > triplet.dstAttr(0).asInstanceOf[Double]) {
              //println(triplet.srcId + " |#| " + triplet.srcAttr(0) +  " |#| " + triplet.srcAttr(1) + " |#| " + triplet.attr + " |#| "+ triplet.dstAttr(0) + " |#| " + triplet.dstAttr(1) + " |#| " + triplet.dstId)
              Iterator((triplet.dstId, Array(triplet.srcAttr(0).asInstanceOf[Double] + triplet.attr.asInstanceOf[Double], triplet.dstAttr(0).asInstanceOf[Double], triplet.srcAttr(2) , hopcount, triplet.srcId)))
            }
            else if (triplet.srcAttr(0).asInstanceOf[Double] + triplet.attr.asInstanceOf[Double]   > triplet.dstAttr(0).asInstanceOf[Double] && triplet.srcAttr(0).asInstanceOf[Double] + triplet.attr.asInstanceOf[Double]  < triplet.dstAttr(1).asInstanceOf[Double]) {
              //println(triplet.srcId + " |$| " + triplet.srcAttr(0) +  " |$| " + triplet.srcAttr(1) + " |$| " + triplet.attr + " |$| "+ triplet.dstAttr(0) + " |$| " + triplet.dstAttr(1) + " |$| " + triplet.dstId)
              Iterator((triplet.dstId, Array(triplet.dstAttr(0).asInstanceOf[Double], triplet.srcAttr(0).asInstanceOf[Double] + triplet.attr.asInstanceOf[Double], triplet.srcAttr(2) , hopcount, triplet.srcId)))
            }
            else {
              Iterator.empty
            }
          }
          else
          {
            //println("empty - " + triplet.srcId + " | " + triplet.srcAttr(0) + " | " + triplet.srcAttr(1) + " | " + triplet.srcAttr(2) + " | " + triplet.attr + " | "+ triplet.dstAttr(0) + " | "+ triplet.dstAttr(1) + " | " +  triplet.dstAttr(2) + " | " + triplet.dstId)
            Iterator.empty
          }

        },

        (a, b) => {
          if (a(0).asInstanceOf[Double] < b(0).asInstanceOf[Double] && a(1).asInstanceOf[Double] < b(1).asInstanceOf[Double])
          { a }
          else if (a(0).asInstanceOf[Double] < b(0).asInstanceOf[Double] && a(1).asInstanceOf[Double] >= b(1).asInstanceOf[Double])
          { Array(a(0), b(1), a(2) , a(3), a(4), b(5)) }
          else if (a(0).asInstanceOf[Double] >= b(0).asInstanceOf[Double] && a(1).asInstanceOf[Double] < b(1).asInstanceOf[Double])
          { Array(b(0), a(1), a(2) , b(3), a(4), a(5)) }
          else if (a(0).asInstanceOf[Double] >= b(0).asInstanceOf[Double] && a(1).asInstanceOf[Double] >= b(1).asInstanceOf[Double])
          { b }
          else
          { b }
        }
      )
      println("--- count of vertices " + tst_msssp.vertices.count())
      val r = tst_msssp.vertices.filter(vertex => vertex._1 == tgtId).take(2)
      //println("r: " +  r.length +  "|"+ r )
      if(r.length == 0)
        return (srcId , tgtId, Double.PositiveInfinity, Double.PositiveInfinity)
      else
        return (srcId , tgtId, r(0)._2(0).asInstanceOf[Double], r(0)._2(1).asInstanceOf[Double])

    }


    for( authors <- myvalues){
      var srcId = authors._1; var tgtId = authors._2
      var tmp = findShortesPathsinMyGraph(srcId, tgtId);
      println("Distances array is like : " + tmp.toString())
      testingDistancesList += tmp
    }

    var trainingResult = spark.sparkContext.parallelize(trainingDistancesList).map(row => row._1 + "|" + row._2 + "|" + row._3 + "|" + row._4)
    trainingResult.saveAsTextFile(const.dstFolderPath + const.shortestpathsFileName + "training");

 */



    val first_training_sp = udf((x: Long, y: Long) => {

      try {
        val paths = trainingGraph.bfs.fromExpr(s"id = $x").toExpr(s"id = $y").maxPathLength(7).run()
        if(!paths.head(2).isEmpty)
        {
          var sp = paths.columns.size/2
          print(s"path found for $x and $y is $sp")
          sp
        }
        else {
          0
        }
      }
      catch
      {
        case npe: NullPointerException =>
          0
      }


    }
    )

    spark.udf.register("first_training_sp", first_training_sp);

    val examples = trainingSamples.limit(100)
    examples.persist()
    examples.count()


    var trainingResult = examples.withColumn("first_shortestpath", first_training_sp(col("authorId1"),col("authorId2")))

    trainingResult.write.option("sep", "|").mode("overwrite").csv(const.dstFolderPath+const.shortestpathsFileName + "training")

  }



}
