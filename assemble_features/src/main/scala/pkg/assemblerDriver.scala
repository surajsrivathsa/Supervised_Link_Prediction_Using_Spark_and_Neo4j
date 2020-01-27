/*
Authors: Suraj
Date: 2020/01/27
Description: Assemble all five features in single fie
 */

package pkg

import org.apache.spark.sql.SparkSession

object assemblerDriver {

  def main(args: Array[String]): Unit = {
    val const = new constantsFile();
    val spark = SparkSession.builder.master(const.master).appName(const.appName).config("spark.memory.fraction", 0.8).config("spark.memory.storageFraction", 0.3).getOrCreate();

    //Setting loglevel to warning as we don't need info
    //.config("spark.executor.memory", const.executor_memory).config("spark.driver.memory", const.driver_memory)
    spark.sparkContext.setLogLevel("WARN")

    val startTime = System.currentTimeMillis()
    val asutils = new assemblerUtilities(spark);
    asutils.assembleTrainingfeatures(const);
    asutils.assembleTestingFeatures(const)
    val stopTime = System.currentTimeMillis();

    println("Time taken for assembling training and testing features in seconds: " + (stopTime-startTime)/1000)


  }

}
