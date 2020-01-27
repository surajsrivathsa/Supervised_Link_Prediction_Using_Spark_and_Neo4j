/*
Authors: Pramod bontha/ Suraj b S
Date: 2020/01/27
Description: Calculate first shortest
 */

package pkg

import org.apache.spark.graphx.PartitionStrategy.EdgePartition2D
import org.apache.spark.graphx.VertexId
import org.apache.spark.graphx.lib.ShortestPaths
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ListBuffer

object fspDriver {


  def main(args: Array[String]):Unit = {
    val const = new constantsFile();
    val spark = SparkSession.builder.master(const.master).appName(const.appName).getOrCreate;

    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    val fsutils = new fspUtilities(spark);

    val all_graphs = fsutils.generateSubgraphsFromFile(const);
    val trainingGraph = all_graphs(0); val testingGraph = all_graphs(1);
    println("Count of edges in training and testing graph are: " + trainingGraph.edges.count() + " | " + testingGraph.edges.count())

    val all_samples = fsutils.generateSamplesFromFile(const);
    val trainingSamples = all_samples(0); val testingSamples = all_samples(1);
    val cc = fsutils.readFlattenedDF(const);

    val newtrainingSamples = fsutils.preCheckCC2(const,cc,trainingSamples);
    val newtestingSamples = fsutils.preCheckCC2(const,cc,testingSamples);

    //Here computing shortest paths are costly, hence we need to reduce the number of samples for which paths are computed.
    //We do this by filtering out any samples that are not in same connected component, If two vertices doesn;'tt belong to same component then path doesn't exists between them
    //hence no need to compute paths for them

    newtrainingSamples.show(10);
    newtestingSamples.show(10);

    fsutils.generateFirstShortestPath(trainingGraph,testingGraph,newtrainingSamples,newtestingSamples,const);

  }

}
