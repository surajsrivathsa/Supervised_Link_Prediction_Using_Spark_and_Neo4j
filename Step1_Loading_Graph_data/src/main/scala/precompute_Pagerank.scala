import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.graphframes._;
import org.apache.spark.sql.types._

class precompute_Pagerank(spark: SparkSession) {

  def calc_PageRank(graph: GraphFrame, const:constantsFile): Unit =
    {

      val results_year1: GraphFrame = graph.filterEdges(const.year1_filter).pageRank.resetProbability(const.pagerank_probability).tol(const.pagerank_tolerance).run();
      results_year1.vertices.toDF("id","label","pagerank").write.option("sep", "|").mode("overwrite").csv(const.dstFolderPath+const.pagerankFileName+const.year1 );

      val results_year2: GraphFrame = graph.filterEdges(const.year2_filter).pageRank.resetProbability(const.pagerank_probability).tol(const.pagerank_tolerance).run();
      results_year2.vertices.toDF("id","label","pagerank").write.option("sep", "|").mode("overwrite").csv(const.dstFolderPath+const.pagerankFileName+const.year2 );

      val results_year3: GraphFrame = graph.filterEdges(const.year3_filter).pageRank.resetProbability(const.pagerank_probability).tol(const.pagerank_tolerance).run();
      results_year3.vertices.toDF("id","label","pagerank").write.option("sep", "|").mode("overwrite").csv(const.dstFolderPath+const.pagerankFileName+const.year3 );
    }

}
