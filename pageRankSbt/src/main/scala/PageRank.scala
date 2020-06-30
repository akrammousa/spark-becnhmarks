import java.io.{File, PrintWriter}
import java.util.Calendar

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.graphx._


object PageRank {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.appName("Page rank").getOrCreate()

    val graph = GraphLoader.edgeListFile(spark.sparkContext, args(0))

    print("read the file")

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

    graph.degrees

    // Number of iterations as the argument
    val staticPageRank = graph.staticPageRank(10)
    staticPageRank.vertices.collect()



    Calendar.getInstance().getTime()
    val pageRank = graph.pageRank(0.001).vertices
    Calendar.getInstance().getTime()

    print("finished execution")

  }

  def writeDFInFile(df: DataFrame , logsPath: String){
    // Creating a file
    val logical_plan_file = new File(logsPath + "logical.txt")

    val physical_plan_file = new File(logsPath +  "physical.txt")

    // Passing reference of file to the printwriter
    val logical_plan_Writer = new PrintWriter(logical_plan_file)

    val physical_plan_Writer = new PrintWriter(physical_plan_file)
    // Passing reference of file to the printwriter
    logical_plan_Writer.write(df.queryExecution.optimizedPlan.toString())

    physical_plan_Writer.write(df.queryExecution.executedPlan.toString())

    // Closing printwriter
    logical_plan_Writer.close()

    physical_plan_Writer.close()
  }

}
