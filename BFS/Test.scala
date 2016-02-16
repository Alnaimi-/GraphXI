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
    val ssc = new StreamingContext(sc, Seconds(5))

    // Define a Static Graph object FROM:
    val users: RDD[(VertexId, String)] = sc.parallelize(Array((1L, "Joe"), (2L, "Jane"), (3L, "Jake"), (4L, "James"), (5L, "FUUUCK")))

    // For the Incremental snapshot wise BFS
    val edges: RDD[Edge[Long]] = sc.parallelize(Array(Edge(1L, 2L, 1L)))
    val edges2: RDD[Edge[Long]] = sc.parallelize(Array(Edge(3L, 4L, 2L)))
    val edges3: RDD[Edge[Long]] = sc.parallelize(Array(Edge(2L, 3L, 3L))) 
    val edges4: RDD[Edge[Long]] = sc.parallelize(Array(Edge(1L, 5L, 4L), Edge(5L, 4L, 4L)))
    var graph = GraphX(users, edges)
    var graph2 = GraphX(users, edges2)
    var graph3 = GraphX(users, edges3)
    var graph4 = GraphX(users, edges4)

    // For the Vector Clock wise BFS
    var edgesClock: RDD[Edge[(Long, Long)]] = sc.parallelize(Array(Edge(1L, 2L, (1L, 2L)), Edge(3L, 4L, (2L, 3L)), Edge(2L, 3L, (3L, 4L)), Edge(1L, 5L, (4L, 4L)), Edge(5L, 4L, (5L, 5L))))
    var graphClock = GraphX(users, edgesClock)

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
            case "shortest" =>  shortest()
            case "shortestC" => shortestClock()
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

    def shortest() { // eventually this will accept a time range as input
        // // perform intial filtering of the edges for each snapshot if not unreachable vetices are marked as infinity.
        // // depends on the input of graph. If the node doesn't exist yet then do we won't actually have it in the snap
        
        // var edges = graph.edges.filter(e => (e.attr.asInstanceOf[Long] >= 1L && e.attr.asInstanceOf[Long] <= 3L))
        // var graph1 = GraphX.fromEdges(edges, "John Doe")
        var shortest1 = GraphX.shortestPath(graph, 1.0)

        var shortest2 = GraphX.shortestPath(graph2, 2.0, Option(shortest1.vertices))

        var shortest3 = GraphX.shortestPath(graph3, 3.0, Option(shortest2.vertices))

        var shortest4 = GraphX.shortestPath(graph4, 4.0, Option(shortest3.vertices))

        shortest4.vertices.foreach(println(_))

    }

    def shortestClock() {
        var shortest = GraphX.shortestPathClock(graphClock)

        shortest.vertices.foreach(println(_))
    }

    def printGraph() {
        println("Vertices:")
        graph.vertices.foreach(x => println(x))

        println("Edges:")
        println(graph.edges.map(_.copy()).count)
        graph.edges.foreach(x => println(x))
    }
}