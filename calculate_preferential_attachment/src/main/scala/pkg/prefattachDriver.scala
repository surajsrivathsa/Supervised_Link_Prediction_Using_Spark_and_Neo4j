/*
Authors: Pramod bontha/ Suraj B S
Date: 2020/01/27
Description: Calculate prefeential attachment
 */

package pkg

import org.apache.spark.sql.SparkSession

object prefattachDriver {

  def main(args: Array[String]): Unit = {
    val const = new constantsFile();
    val spark = SparkSession.builder.master(const.master).appName(const.appName)getOrCreate();

    //Setting loglevel to warning as we don't need info
    //.config("spark.executor.memory", const.executor_memory).config("spark.driver.memory", const.driver_memory)
    spark.sparkContext.setLogLevel("WARN")
    val pautils = new prefattachUtilities(spark)
    val all_graphs = pautils.generateSubgraphsFromFile(const);
    val trainingGraph = all_graphs(0); val testingGraph = all_graphs(1);

    pautils.preferentialAttachment(trainingGraph,testingGraph,const)

    val complete_graph = pautils.createCompleteGraphFromFile(const)
    pautils.globalpreferentialAttachment(complete_graph,const)

  }

}
