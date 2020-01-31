package pkg

import org.apache.spark.graphx.PartitionStrategy.EdgePartition2D
import org.apache.spark.graphx.VertexId
import org.apache.spark.graphx.lib.ShortestPaths
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ListBuffer

object ccfDriver {
  def main(args: Array[String]):Unit = {
    val const = new constantsFile();
    val spark = SparkSession.builder.master(const.master).appName(const.appName).getOrCreate;

    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._
    val ccfutils = new ccfUtilities(spark);

    //read subgraphs
    val all_graphs = ccfutils.generateSubgraphsFromFile(const)
    val trainingGraph = all_graphs(0); val testingGraph = all_graphs(1);

    val all_samples = ccfutils.generateSamplesFromFile(const);
    val trainingSamples = all_samples(0); val testingSamples = all_samples(1);

    //generate training cliques and run connected compoinents for training cliques
    val trainingcliques = ccfutils.generateTrainingCliques(const,trainingGraph);
    ccfutils.runConnectedComponentsForTraining(const, trainingcliques);

    //generate testing cliques and run connected compoinents for testing cliques
    val testingcliques = ccfutils.generateTestingCliques(const,testingGraph);
    ccfutils.runConnectedComponentsForTesting(const, testingcliques);

    //Read this written connected components file separately for training and testing
    var training_fdf = ccfutils.readFlattenedTrainingDF(const);
    var testing_fdf = ccfutils.readFlattenedTestingDF(const);


    //Generate connected component feature for training and testing
    ccfutils.createccfTraining(const,training_fdf,trainingSamples);
    ccfutils.createccfTesting(const,testing_fdf,testingSamples);

  }
}



