import java.util

import org.apache.spark._
import scala.collection.mutable
import scala.util.control.NonFatal
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
import org.apache.spark.streaming.dstream.DStream
import java.util.Date
import java.io.{PrintWriter, File}
import scala.collection.mutable.{ArrayBuffer, HashSet}
import java.util.ArrayList
import java.io._

object Test2 {
  val sparkConf = new SparkConf().setAppName("Snapshotter")
    
  val sc  = new SparkContext(sparkConf)
  val ssc = new StreamingContext(sc, Seconds(15))
  
  var count = 0

  // Turn off the 100's of messages
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  // Create empty graph
  val initUsers: RDD[(VertexId, Int)] = sc.parallelize(Array.empty[(VertexId, Int)])
  val initEdges: RDD[Edge[(Long, Long)]] = sc.parallelize(Array.empty[Edge[(Long, Long)]])
  var mainGraph: Graph[Int, (Long, Long)] = GraphX(initUsers, initEdges)

  def main(args: Array[String]) {

    var stream: DStream[String] = null

    args.length match {
      case 2 => stream = ssc.socketTextStream(args(0), args(1).toInt, StorageLevel.MEMORY_AND_DISK_SER)
      case 1 => stream = ssc.textFileStream("hdfs://moonshot-ha-nameservice/user/bas30/output2/")
      case _ => println("Incorrect num of args, please refer to readme.md!")
    }

    extractRDD(stream) // Extract RDD inside the dstream
    
    // Run stream and await termination
    ssc.start()
    ssc.awaitTermination()
  }

  var timestamp = 1L
  def extractRDD(stream: DStream[String]){
    // Put RDD in scope
    stream.foreachRDD(rdd => {
      count = count + 1 // iterations
      println("Iteration " + count)

      if (!rdd.isEmpty) { // causes emtpy collection exception
        timestamp = timestamp + 1L
        // found   : org.apache.spark.graphx.VertexRDD[Int]
        // required: org.apache.spark.rdd.RDD[(org.apache.spark.graphx.VertexId, org.apache.spark.graphx.VertexId)]
        mainGraph = parseCommands(rdd, mainGraph)
      }

      println("End...")
      status(mainGraph)
    })
  }

  def parseCommands(rdd: RDD[String], graph: Graph[Int, (Long, Long)]): Graph[Int, (Long, Long)] = {
    val startTime = System.currentTimeMillis

    // reduce the RDD and return a tuple
    var reduced = reduceRDD(rdd)
    var addEdgeSet = reduced._1
    var rmvEdgeSet = reduced._2
    var addNodeSet = reduced._3
    var rmvNodeSet = reduced._4

    val endTime = System.currentTimeMillis - startTime
    println("reduce time = " + endTime)

    val startTime2 = System.currentTimeMillis
    
    mainGraph = graphRemove(mainGraph, rmvEdgeSet, rmvNodeSet)
    mainGraph = graphAdd(mainGraph, addEdgeSet, addNodeSet)

    val endTime2 = System.currentTimeMillis - startTime2
    println("build time = " + endTime2)

    mainGraph
  }

  def graphRemove(
    graph: Graph[Int, (Long, Long)], 
    rmvEdgeSet: HashSet[String], 
    rmvNodeSet: HashSet[String]): Graph[Int, (Long, Long)] = {

		@transient val newEdges = graph.edges.map(edge => checkEdge(edge, new HashSet[String]()))

    GraphX(graph.vertices, newEdges)
  }

  def checkEdge(edge: Edge[(Long, Long)], rmvEdgeSet: HashSet[String]): Edge[(Long, Long)] = {
		return edge 
  }

  def graphAdd(
    graph: Graph[Int, (Long, Long)], 
    addEdgeSet: HashSet[String], 
    addNodeSet: HashSet[String]): Graph[Int, (Long, Long)] = {

    var edgeArray: Array[Edge[(Long, Long)]] = addEdgeSet.toArray.map(edge => {
      var command = edge.split(" ")
      Edge(command(1).toLong, command(3).toLong, (timestamp, Long.MaxValue))
    })

    var vertArray = addNodeSet.toArray.map(vert => {
      var command = vert.split(" ")
      (command(1).toLong, 1)
    })

    var vertices = sc.parallelize(vertArray)
    var edges = sc.parallelize(edgeArray)

    // reduces down to distinct based on edge attribute
    GraphX(
      graph.vertices.union(vertices), 
      graph.edges.union(edges)
      .map(x => ((x.srcId, x.dstId, x.attr._2), x))
      .reduceByKey((a, b) => if(a.attr._1 < b.attr._1) a else b)
      .map(_._2)
    )
  }

  def saveGraph(){
    mainGraph.edges.foreach(println(_))

    mainGraph.vertices.saveAsTextFile("prev/" + timestamp.toString + "/vertices")
    mainGraph.edges.saveAsTextFile("prev/" + timestamp.toString + "/edges")
  }

  def reduceRDD(rdd: RDD[String]): (HashSet[String], HashSet[String], HashSet[String], HashSet[String]) = {
    // use hashset for its distinct items
    var addEdgeSet = new HashSet[String]()
    var addNodeSet = new HashSet[String]()
    var rmvEdgeSet = new HashSet[String]()
    var rmvNodeSet = new HashSet[String]()

    val rddArray = rdd.collect()

    for (i <- (rddArray.length - 1) to 0 by -1) {
      val split = rddArray(i).split(" ")

      val command = split(0)
      val src = split(1)
      var dst = ""
      var msg = ""

      if(split.length > 3) {
        msg = split(2)
        dst = split(3)
      }

      command match {
        case "addEdge" => {
          if (rmvNodeSet.contains("rmvNode " + src)) { // check if the src is removed lower down
            if (!rmvNodeSet.contains("rmvNode " + dst)) { // check if the dst is also removed, if not
              addNodeSet.add("addNode " + dst)
            }
          } else if (rmvNodeSet.contains("rmvNode " + dst)) { // check if the dst is removed lower down
            addNodeSet.add("addNode " + src)
          } else if (rmvEdgeSet.contains("rmvEdge " + src + " " + msg + " " + dst)) {
            addNodeSet.add("addNode " + src) //no need to check if they are negated as it is checked above
            addNodeSet.add("addNode " + dst)
          } else { //if there are no remove nodes or edges then we can add the command to the subset
            addEdgeSet.add(rddArray(i))
          }
        }
        case "rmvEdge" => {
          if(!addEdgeSet.contains("addEdge " + src + " " + msg + " " + dst) &&
             !rmvNodeSet.contains("rmvNode " + src) && !rmvNodeSet.contains("rmvNode " + dst)) {
            rmvEdgeSet.add(rddArray(i))
          }  
        }
        case "addNode" => if (!rmvNodeSet.contains("rmvNode " + src)) addNodeSet.add(rddArray(i))
        case "rmvNode" => rmvNodeSet.add(rddArray(i)) // rmvNode  can't be contra
        case _ => println("The operation " + command + " isn't valid.")
      }
    }

    (addEdgeSet, rmvEdgeSet, addNodeSet, rmvNodeSet)
  }

  def graphEqual(graph1: Graph[Int, (Long, Long)], graph2: Graph[Int, (Long, Long)]): Boolean = {
    var thisV = graph1.vertices
    var thisE = graph1.edges

    var otherV = graph2.vertices
    var otherE = graph2.edges

    thisV.count == (thisV.intersection(otherV).count) && thisE.count == (thisE.intersection(otherE).count)
  } 

  def status(graph: Graph[Int, (Long, Long)]) {
    println("Performing batch processing...")
    println("edge total: " + graph.numEdges.toInt)

    println("vertex total: " + graph.numVertices.toInt)
  }
}
