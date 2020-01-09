package pkg

import org.apache.spark.sql._
import org.graphframes._

class precompute_Pagerank(spark: SparkSession) {

  def calc_PageRank(graph: GraphFrame, const:constantsFile): Unit =
    {

      import spark.implicits._

      println("The time at begining of pagerank1 is: " + System.currentTimeMillis)

      val filtered_graph_year1 = graph.filterEdges(const.year1_filter)
      val results_graphx_year1 = filtered_graph_year1.toGraphX
      results_graphx_year1.persist();

      println("------- Persisted graph of year1 in pagerank, count of the vertices is --------------- " + results_graphx_year1.vertices.count())

      val pagerank_year1 = results_graphx_year1.staticPageRank(const.pagerank_iteration,const.pagerank_probability)
      pagerank_year1.persist();
      pagerank_year1.vertices.map(row => row._1.asInstanceOf[Long] + "," + row._2).saveAsTextFile(const.dstFolderPath+const.pagerankFileName+const.year1);


      println("The time at end of pagerank1 is: " + System.currentTimeMillis)

      val filtered_graph_year2 = graph.filterEdges(const.year2_filter)
      val results_graphx_year2 = filtered_graph_year2.toGraphX
      results_graphx_year2.persist();

      println("-------- Persisted graph of year2 in pagerank, count of the vertices is ----------- "  + results_graphx_year2.vertices.count())

      val pagerank_year2 = results_graphx_year2.staticPageRank(const.pagerank_iteration,const.pagerank_probability)
      pagerank_year2.persist();
      pagerank_year2.vertices.map(row => row._1.asInstanceOf[Long] + "," + row._2).saveAsTextFile(const.dstFolderPath+const.pagerankFileName+const.year2);

      println("The time at end of pagerank2 is: " + System.currentTimeMillis)


/*
      val filtered_graph_year3 = graph.filterEdges(const.year3_filter)
      val results_graphx_year3 = filtered_graph_year3.toGraphX
      results_graphx_year3.persist();

      println("-------- Persisted graph of year3 in pagerank, count of the vertices is ----------- " + results_graphx_year3.vertices.count())

      val pagerank_year3 = results_graphx_year3.staticPageRank(const.pagerank_iteration,const.pagerank_probability)
      pagerank_year3.persist();
      pagerank_year3.vertices.map(row => row._1.asInstanceOf[Long] + "," + row._2).saveAsTextFile(const.dstFolderPath+const.pagerankFileName+const.year3);


 */

      println("The time at end of pagerank3 is: " + System.currentTimeMillis)

      println("---------------  Count of pagerank vertices for year1, year2 and year3 are: " + pagerank_year1.vertices.count() + " , " + pagerank_year2.vertices.count() + " , " + -1 + " ----------------" )

      /*
            val results_year1: GraphFrame = filtered_graph_year1.pageRank.resetProbability(const.pagerank_probability).maxIter(const.iteration).run();
            results_year1.vertices.toDF("id","label","pagerank").write.option("sep", "|").mode("overwrite").csv(const.dstFolderPath+const.pagerankFileName+const.year1 );


            val filtered_graph_year2 = graph.filterEdges(const.year2_filter)
            filtered_graph_year2.persist();
            val results_year2: GraphFrame = filtered_graph_year2.pageRank.resetProbability(const.pagerank_probability).maxIter(const.iteration).run();
            results_year2.vertices.toDF("id","label","pagerank").write.option("sep", "|").mode("overwrite").csv(const.dstFolderPath+const.pagerankFileName+const.year2 );


            val filtered_graph_year3 = graph.filterEdges(const.year3_filter)
            filtered_graph_year3.persist();
            val results_year3: GraphFrame = filtered_graph_year3.pageRank.resetProbability(const.pagerank_probability).maxIter(const.iteration).run();
            results_year3.vertices.toDF("id","label","pagerank").write.option("sep", "|").mode("overwrite").csv(const.dstFolderPath+const.pagerankFileName+const.year3 );

       */


    }

}
