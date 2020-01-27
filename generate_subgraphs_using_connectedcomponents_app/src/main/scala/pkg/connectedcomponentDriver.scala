/*
Author: Suraj.B.S
Date: 2020/01/27
Description: Sample the graph and generate connected components. Write selected number of components to file
 */

package pkg

import org.apache.spark.sql.SparkSession

object connectedcomponentDriver {

  def main(args: Array[String]): Unit =
  {
    val const = new constantsFile();
    val spark = SparkSession.builder.master(const.master).appName(const.appName).config("spark.memory.fraction",0.8).config("spark.memory.storageFraction",0.3).getOrCreate();

    //Setting loglevel to warning as we don't need info
    //.config("spark.executor.memory", const.executor_memory).config("spark.driver.memory", const.driver_memory)
    spark.sparkContext.setLogLevel("WARN")

    val startTime = System.currentTimeMillis()

    val ccutils = new connectedcomponentUtilities(spark);
    val graph = ccutils.createCompleteGraphFromFile(const);
    val cliques = ccutils.generateCliques(const,graph);
    ccutils.runConnectedComponents(const, cliques);

    //val connected_components = ccutils.createDfFromCC2File(const);

    val connected_components = ccutils.readFlattenedDF(const);

    ccutils.createSubGraphsFromCC2andCompleteGraph(const,graph,connected_components);


    val stopTime = System.currentTimeMillis()

    println("Time taken for CC2 program to complete in seconds: " + (stopTime-startTime)/1000)


  }

}
