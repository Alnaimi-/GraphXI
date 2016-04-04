import java.util

import org.apache.spark._
import scala.collection.mutable
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.collection.mutable.HashSet
import org.apache.hadoop.fs.Path

object GraphBuilder {
  val sparkConf = new SparkConf().setAppName("GraphBuilder")
  val sc = new SparkContext(sparkConf)
	
  val fs = org.apache.hadoop.fs.FileSystem.get(sc.hadoopConfiguration)

  // Turn off log messages
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  // Create empty graph
  var mainGraph: Graph[Int, (Long, Long)] = initGraph()
  var batchCount = 0
  var timestamp = 1L

  def main(args: Array[String]) {
    while(true){			
      if(fs.exists(new org.apache.hadoop.fs.Path("/user/bas30/output/"+batchCount))){
        fileStream()
      }

      if(timestamp % 5 == 0) saveGraph() // save graph every 5
    }
  }

  def fileStream(){
    println("Timestamp: " + timestamp)
		
	val batch: RDD[String] = sc.textFile("/user/bas30/output/"+batchCount, 10)		
	batch.cache()

	val rmvEdgeRAW = batch.filter(string => string.contains("rmvEdge"))
	val rmvNodeRAW = batch.filter(string => string.contains("rmvNode"))
	val addEdgeRAW = batch.filter(string => string.contains("addEdge"))
	val addNodeRAW = batch.filter(string => string.contains("addNode"))

	val buildTime = System.currentTimeMillis()

    mainGraph = graphRemove(mainGraph, rmvEdgeRAW, rmvNodeRAW)
    mainGraph = graphAdd(mainGraph, addEdgeRAW, addNodeRAW)
	
	mainGraph.cache
	status(mainGraph)
	
	println("Build time: " + (System.currentTimeMillis() - buildTime))

	timestamp = timestamp+1
	batchCount = batchCount+1 		
  }

  def graphRemove (
    graph: Graph[Int, (Long, Long)], 
    rmvEdgeRAW: RDD[String], 
    rmvNodeRAW: RDD[String]): Graph[Int, (Long, Long)] = {
    
    val t = timestamp

    val rmvEdges = rmvEdgeRAW.map(string => {
      val split = string.split(" ")
      val srcId = split(1).toLong
      val dstId = split(3).toLong

      (srcId, dstId)
    })

    val rmvNodes = rmvNodeRAW.map(str => str.split(" ")(1).toLong)

    val edges = graph.edges

    var newEdges: RDD[Edge[(Long, Long)]] = (edges.map(v => (v.srcId, v)).union(edges.map(v => (v.dstId, v))))
    .cogroup(rmvNodes.map(v => (v, null)))
    .filter { case (_, (leftGroup, rightGroup)) => leftGroup.nonEmpty}
    .map { case (_, (leftGroup, rightGroup)) => {
      val edge = leftGroup.last
      val src = edge.srcId
      val dst = edge.dstId
      var att = edge.attr

      if(rightGroup.nonEmpty) {
        if(att._2 == Long.MaxValue)
          att = (att._1, t)
      }

      Edge(src, dst, att)
    }}
    .map(v => ((v.srcId, v.dstId), v))
    .cogroup(rmvEdges.map(v => ((v._1, v._2), null)))
    .filter { case (_, (leftGroup, rightGroup)) => leftGroup.nonEmpty}
    .map { case (_, (leftGroup, rightGroup)) => {
      val edge = leftGroup.last
      val src = edge.srcId
      val dst = edge.dstId
      var att = edge.attr

      if(rightGroup.nonEmpty) {
        if(att._2 == Long.MaxValue)
          att = (att._1, t)
      }

      Edge(src, dst, att)
    }}

    GraphX(graph.vertices, newEdges.distinct)
  }

  def graphAdd(
    graph: Graph[Int, (Long, Long)], 
    addEdgeRAW: RDD[String], 
    addNodeRAW: RDD[String]): Graph[Int, (Long, Long)] = {

  	val t = timestamp

    val edges: RDD[Edge[(Long, Long)]] = addEdgeRAW.map(edge => {
      val command = edge.split(" ")
      Edge(command(1).toLong, command(3).toLong, (t, Long.MaxValue))
    })

    val vertices: RDD[(VertexId, Int)] = addNodeRAW.map(vert => {
      val command = vert.split(" ")
      (command(1).toLong, 1)
    })

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

    mainGraph.vertices.saveAsTextFile("BFS/prev/shortest" + timestamp.toString + "/vertices")
    mainGraph.edges.saveAsTextFile("BFS/prev/shortest" + timestamp.toString + "/edges")
  }

  def status(graph: Graph[Int, (Long, Long)]) {
    println("Performing batch processing...")

    println("edge total: " + graph.numEdges.toInt)
    println("vertex total: " + graph.numVertices.toInt)
  }

  def initGraph(): Graph[Int, (Long, Long)] = {
    val vertices: RDD[(VertexId, Int)] = sc.parallelize(Array.empty[(VertexId, Int)])
    val edges: RDD[Edge[(Long, Long)]] = sc.parallelize(Array.empty[Edge[(Long, Long)]])

	Graph(vertices, edges)
  }
}
