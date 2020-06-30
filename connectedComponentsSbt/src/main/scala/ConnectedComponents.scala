import java.io.{File, PrintWriter}
import java.util.Calendar

import org.apache.spark.sql.{DataFrame, SparkSession}
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

    val vertices = cc.vertices
    vertices.count()


    val vertexCount = cc.numVertices
    println("cc vertex count is" + vertexCount)

    val edgeCount = cc.numEdges
    println("cc edges count is" + edgeCount)

    cc.vertices.collect()

    // Print top 5 items from the result
    //println(cc.vertices.take(5).mkString("\n"))

    val scc = graph.stronglyConnectedComponents(args(1).toInt)
    scc.vertices.collect()

    val vertexCountscc = scc.numVertices
    println("scc vertex count is" + vertexCountscc)


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
