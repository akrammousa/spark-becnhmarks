import java.util.Calendar

import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx._

// data link : https://snap.stanford.edu/data

object ConnectedComponents {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.appName("connected components").getOrCreate()
    // Connected Components
    val graph = GraphLoader.edgeListFile(spark.sparkContext, args(0))

    Calendar.getInstance().getTime()
    val cc = graph.connectedComponents()
    Calendar.getInstance().getTime()

    cc.vertices.collect()

    // Print top 5 items from the result
    //println(cc.vertices.take(5).mkString("\n"))

    val scc = graph.stronglyConnectedComponents(args(1).toInt)
    scc.vertices.collect()
  }
}
