import org.apache.spark._
import org.apache.spark.SparkConf
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.log4j.Logger
import org.apache.log4j.Level

object ShortestPath {
  val sparkConf = new SparkConf().setAppName("StreamX")
  val sc  = new SparkContext(sparkConf)

  // Turn off the 100's of messages
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  def main(args: Array[String]) {
    val graph = readGraph(args(0).toLong, args(1).toLong)
    val startTime = System.currentTimeMillis
    val short = GraphX.shortestPathClock(graph)
    println("Time taken: " + ((System.currentTimeMillis - startTime) / 1000))
    
    short.vertices.saveAsTextFile("shortest")
  }

  def readGraph(t1: Long, t2: Long): Graph[Int, (Long, Long)] = {
    var end = (Math.ceil(t2/5.0) * 5).toInt.toLong
    val vertx = sc.textFile("prev/shortest" + end + "/vertices")
    
    val vertRDD: RDD[(VertexId, Int)] = vertx.map(line => {
      val split = line.split(",")
      (split(0).substring(1).toLong, 0) // and turn back into a Vertex
    })

    val edges = sc.textFile("prev/shortest" + end + "/edges")
    val edgeRDD: RDD[Edge[(Long, Long)]] = edges.map(line => {
      // Edge(1,3,(4,8)) => Array(1,3,4,8)
      val split = line.replaceAll("Edge|[()]", "").split(",")
      val src = split(0).toLong
      val dst = split(1).toLong
      val att = (split(2).toLong, split(3).toLong)
      Edge(src, dst, att)
    }).filter(e => e.attr._1 >= t1 && e.attr._1 < t2)

    println("Num of vertices: " + vertRDD.count)
    println("Num of edges:    " + edgeRDD.count)

    GraphX(vertRDD, edgeRDD)
  }
}