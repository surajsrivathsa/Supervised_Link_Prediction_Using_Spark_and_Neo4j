/*
Authors: Pramod bontha/ Suraj b S
Date: 2020/01/27
Description: Calculate common neighbours
 */

package pkg

import org.apache.spark.sql.SparkSession

object commonneighboursDriver {

  def main(args: Array[String]): Unit = {
    val const = new constantsFile();
    val spark = SparkSession.builder.master(const.master).appName(const.appName).config("spark.memory.fraction", 0.8).config("spark.memory.storageFraction", 0.3).getOrCreate();

    //Setting loglevel to warning as we don't need info
    //.config("spark.executor.memory", const.executor_memory).config("spark.driver.memory", const.driver_memory)
    spark.sparkContext.setLogLevel("WARN")

    val cnutils = new commonneighboursUtilities(spark);
    val all_graphs = cnutils.generateSubgraphsFromFile(const)
    val all_samples = cnutils.generateSamplesFromFile(const)
    val trainingGraph = all_graphs(0); val testingGraph = all_graphs(1)
    val trainingSamples = all_samples(0); val testingSamples = all_samples(1);

    cnutils.generateCommonNeighbours(trainingGraph,testingGraph,trainingSamples,testingSamples,const)

  }

}
