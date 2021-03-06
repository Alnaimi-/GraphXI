import org.apache.spark._
import scala.util.control.NonFatal
// To make some of the examples work we will also need RDD
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.storage.StorageLevel
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.HashPartitioner

object NetworkWordCount {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: NetworkWordCount <hostname> <port>")
      System.exit(1)
    }

    // Create the context with a 1 second batch size
    val sparkConf = new SparkConf().setAppName("NetworkWordCount")
    val ssc = new StreamingContext(sparkConf, Seconds(10))

   
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    // Create a socket stream on target ip:port and count the
    // words in input stream of \n delimited text (eg. generated by 'nc')
    // Note that no duplication in storage level only for running locally.
    // Replication necessary in distributed scenario for fault tolerance.
    val lines = ssc.socketTextStream(args(0), args(1).toInt, StorageLevel.MEMORY_AND_DISK_SER)
    //lines.saveAsTextFiles("test","")
   
    lines.foreachRDD( rdd => {
      if(!rdd.partitions.isEmpty){
        val edges: RDD[Edge[String]] = rdd.map { line =>
          val fields = line.split(" ")
          Edge(fields(0).toLong, fields(2).toLong, fields(1))
        }
        var graph : Graph[VertexId, String] = Graph.fromEdges(edges, 1L)
        println("num edges = " + graph.numEdges);
        println("num vertices = " + graph.numVertices);
      }
    })
   
    
    /*val edges: 
*/
    ssc.start()
    ssc.awaitTermination()
  }
}