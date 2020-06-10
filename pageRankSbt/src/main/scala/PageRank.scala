import java.util.Calendar
import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx._


object PageRank {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.appName("Page rank").getOrCreate()

    val graph = GraphLoader.edgeListFile(spark.sparkContext, args(0))

    // Compute the graph details like edges, vertices etc.

    val vertexCount = graph.numVertices
    println("vertex count is" + vertexCount)

    val vertices = graph.vertices
    vertices.count()

    val edgeCount = graph.numEdges
    println("edges count is" + edgeCount)


    val edges = graph.edges
    edges.count()

    //
    // Let's look at some of the Spark GraphX API like triplets, indegrees, and outdegrees.
    //
    val triplets = graph.triplets
    triplets.count()
    triplets.take(5)

    val inDegrees = graph.inDegrees
    inDegrees.collect()

    val outDegrees = graph.outDegrees
    outDegrees.collect()

    val degrees = graph.degrees
    degrees.collect()

    // Number of iterations as the argument
    val staticPageRank = graph.staticPageRank(10)
    staticPageRank.vertices.collect()

    Calendar.getInstance().getTime()
    val pageRank = graph.pageRank(0.001).vertices
    Calendar.getInstance().getTime()

    print("finished execution")

  }

}
