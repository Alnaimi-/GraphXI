import org.apache.spark._
import org.apache.spark.SparkConf
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.log4j.Logger
import org.apache.log4j.Level

import java.util.Calendar

object Test {
    val sparkConf = new SparkConf().setAppName("StreamX")
    
    val sc  = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(10))

    // Define an empty Graph object
    val users: RDD[(VertexId, String)] = sc.parallelize(Array.empty[(VertexId, String)])
    val edges: RDD[Edge[Long]] = sc.parallelize(Array.empty[Edge[Long]])
    var graph = GraphX(users, edges)
    var timestamp = 1L

    // Turn off the 100's of messages
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    def main(args: Array[String]) {
        // Define a DStream
        var stream: DStream[String] = null

        args.length match {
            case 2 => stream = ssc.socketTextStream(args(0), args(1).toInt, StorageLevel.MEMORY_AND_DISK_SER)
            case 1 => stream = ssc.textFileStream(args(0))
            case _ => println("Incorrect num of args, please refer to readme.md!")
        }

        readStream(stream)

        ssc.start
        ssc.awaitTermination
    }

    // Read the data from stream
    def readStream(stream: DStream[String]) {
        stream.foreachRDD(rdd => {
            println("Starting new batch...")

            if(!rdd.isEmpty) {
                val spout: List[String] = rdd.collect().toList

                graphRecursion(spout)
            }

            // printGraph()
        })
    }

    // map += {"loadG" -> loadGraph}
    // map += {"viewG" -> viewGraph}  viewG yyyy-MM-dd HH:mm:ss

    def graphRecursion(spout: List[String]) {
        val head = spout.head
        val tail = spout.tail

        val data = head.split(" ")

        // Get operation command
        val operation = data.head

        operation match {
            case "addN" => addN(data)
            case "delN" => delN(data)
            case "addE" => addE(data)
            case "delE" => delE(data)
            case "earliest" =>  earliest()
            case _ => 
                println("The method " + operation + " isn't valid.")
                if (!spout.tail.isEmpty)
                    graphRecursion(tail)
        }
        
        if (!spout.tail.isEmpty)
            graphRecursion(tail)
    }

    def addN(data: Array[String]) {
        val srcId = data.tail(0).toLong
        val name  = data.tail(1)
        var vertex  = sc.parallelize(Array((srcId, name)))
        
        graph = GraphX.addNode(graph, vertex)
    }

    def delN(data: Array[String]) {
        val srcId = data.tail(0).toLong
        val name  = data.tail(1)
        var vertex  = sc.parallelize(Array((srcId, name)))      

        graph = GraphX.removeNode(graph, vertex)
    }

    def addE(data: Array[String]) {
        var srcId = data.tail(0).toLong
        var dstId = data.tail(1).toLong
        var msg   = data.tail(2).toLong

        var edgeRDD = sc.parallelize(Array(Edge(srcId, dstId, msg)))

        graph = GraphX.addEdge(graph, edgeRDD, "John Doe")
    }

    def delE(data: Array[String]) {
        val srcId = data.tail(0).toLong
        var dstId = data.tail(1).toLong
        var msg   = data.tail(2).toLong

        var edgeRDD = sc.parallelize(Array(Edge(srcId, dstId, msg)))

        graph = GraphX.removeEdge(graph, edgeRDD)
    }

    def earliest() {
        var src = 1L
        var timeInterval = (1L, 4L)

        var x = GraphX.earliestArrival(graph, src, timeInterval)    
        println(x)
        // graph.edges.sortBy(e => (e.attr.asInstanceOf[Long], e.srcId.asInstanceOf[Long])).collect.foreach(edge => println(edge))
    }

    def printGraph() {
        println("Vertices:")
        graph.vertices.foreach(x => println(x))

        println("Edges:")
        println(graph.edges.map(_.copy()).count)
        graph.edges.foreach(x => println(x))
    }
}