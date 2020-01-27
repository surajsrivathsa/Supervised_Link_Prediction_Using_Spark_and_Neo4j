/*
Author: Suraj.B.s
Date: 2020/01/27
Description: Generate Training and testing Samples and graphs
 */

package pkg

import org.apache.spark.sql.SparkSession

object generatesamplesDriver {
  def main(args: Array[String]): Unit = {
    val const = new constantsFile();
    val spark = SparkSession.builder.master(const.master).appName(const.appName).getOrCreate();

    //Setting loglevel to warning as we don't need info
    //.config("spark.executor.memory", const.executor_memory).config("spark.driver.memory", const.driver_memory)
    spark.sparkContext.setLogLevel("WARN")

    val gsutil = new gsUtilities(spark);
    val graph = gsutil.createCompleteGraphFromFile(const)

    gsutil.generateSamples(graph,const);
    gsutil.generateSubGraphs(graph,const);

  }


}
