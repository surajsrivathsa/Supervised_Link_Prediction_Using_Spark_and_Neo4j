/*
Author: Pramod Bontha/Suraj.B.S
Date: 2020/01/27
Description: Calcu;ate pagerank for training and testing and global data
 */

package pkg

import org.apache.spark.sql.SparkSession

object pagerankDriver {

  def main(args: Array[String]):Unit = {

    val const = new constantsFile();
    val spark = SparkSession.builder.master(const.master).appName(const.appName).getOrCreate();

    spark.sparkContext.setLogLevel("WARN")

    val pgutils = new pagerankUtilities(spark);
    val all_graphs = pgutils.generateSubgraphsFromFile(const);
    val trainingGraph = all_graphs(0); val testingGraph = all_graphs(1);

    pgutils.calc_DynamicPageRank(trainingGraph,testingGraph,const)
    pgutils.calc_StaticPageRank(trainingGraph,testingGraph,const)

    val complete_graph = pgutils.createCompleteGraphFromFile(const);
    pgutils.calcDynamicGlobalPageRank(complete_graph,const);


  }

}
