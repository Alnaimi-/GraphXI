import org.apache.spark._
import org.apache.spark.SparkConf
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.log4j.Logger
import org.apache.log4j.Level

// scheduling
import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext
import ExecutionContext.Implicits.global

// write to file
import java.io.{File, FileWriter, FileNotFoundException, PrintWriter}

import collection.mutable.HashMap


object Test {
    val sparkConf = new SparkConf().setAppName("StreamX")
    
    val sc  = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(10))
    
    // schedule the snapshotter every 5sec
    val system = akka.actor.ActorSystem("system")
    system.scheduler.schedule(0 seconds, 5 seconds)(snapshotter("snapshots"))

    // Define an empty Graph object
    val users: RDD[(VertexId, String)] = sc.parallelize(Array.empty[(VertexId, String)])
    val edges: RDD[Edge[String]] = sc.parallelize(Array.empty[Edge[String]])
    var graph = GraphX(users, edges)

    // every 10 seconds u snapshot
    /* for each vertex in the graph
     * you dump the vertex followed by    V0
     * the edges belonging to that vertex C0
     * then all the activities sorted by  A01-A0N
     * timestamp.
     * The graph will contain all vertices 
     * and edges we need to keep track of 
     * all activities. Have an hashmap
     * that stores the key (vertex Id)
     * and the value (activity)
     * Keep the activity list as is, so it 
     * evolves with the graph.
     */

    // Turn off the 100's of messages
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val activities = new HashMap[Long, RDD[String]]
    var newActivity = false
    var timestamp = 0

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

                newActivity = true
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

        activities += ((srcId -> sc.parallelize(Array.empty[String])))
    }

    def delN(data: Array[String]) {
        val srcId = data.tail(0).toLong
        val name  = data.tail(1)
        var vertex  = sc.parallelize(Array((srcId, name)))      

        graph = GraphX.removeNode(graph, vertex)
    }

    def addE(data: Array[String]) {
        val srcId = data.tail(0).toLong
        var dstId = data.tail(1).toLong
        var msg   = data.tail(2)
        var edgeRDD = sc.parallelize(Array(Edge(srcId, dstId, msg)))

        graph = GraphX.addEdge(graph, edgeRDD, "John Doe")

        var srcActivities = activities.get(srcId).get.collect
        activities.update(srcId, sc.parallelize(srcActivities :+ "addE, (v" + srcId + ", v" + dstId + ", " + msg + "))"))
    }

    def delE(data: Array[String]) {
        val srcId = data.tail(0).toLong
        var dstId = data.tail(1).toLong
        var msg   = data.tail(2)
        var edgeRDD = sc.parallelize(Array(Edge(srcId, dstId, msg)))

        graph = GraphX.removeEdge(graph, edgeRDD)
    }

    def printGraph() {
        println("Vertices:")
        graph.vertices.foreach(x => println(x))

        println("Edges:")
        println(graph.edges.map(_.copy()).distinct.count)
        graph.edges.foreach(x => println(x))
    }

    def snapshotter(dir: String) {
        if (newActivity) {
            graph.vertices.collect().foreach(x => {
                var vId = x._1

                withPrintWriter(dir + "/t" + timestamp, "v" + vId + ".txt") { printWriter =>
                    printWriter.write(x + ":{\n")
                    printWriter.write("  c" + vId +":{\n")
                }
                graph.triplets
                    .filter(triplet => 
                        triplet.srcId == vId || triplet.dstId == vId
                    )
                    .collect
                    .foreach(triplet => {
                        withPrintWriter(dir + "/t" + timestamp, "v" + vId + ".txt") { printWriter =>
                            printWriter.write(" " + triplet + "\n")
                        }
                    })
                withPrintWriter(dir + "/t" + timestamp, "v"+ vId + ".txt") { printWriter =>
                    printWriter.write("  }\n}\n\n")
                }

                activities.get(vId).get.collect.foreach(activity => {
                    var t = 1
                    withPrintWriter(dir + "/t" + timestamp, "v" + vId + ".txt") { printWriter =>
                        printWriter.write("a" + vId + t + " = " + activity + "\n")
                    }
                    t = t+1
                })
            })

            graph.edges.saveAsTextFile(dir + "/t" + timestamp + "/edges")
            graph.vertices.saveAsTextFile(dir + "/t" + timestamp + "/vertices")

            newActivity = false
            timestamp = timestamp+1
        }
    }

    def withPrintWriter(dir: String, name: String)(f: (PrintWriter) => Any) {
        new java.io.File(dir).mkdirs

        val file = new File(dir, name)
        val writer = new FileWriter(file, true)
        val printWriter = new PrintWriter(writer)
        try {
            f(printWriter)
        }
        finally {
            printWriter.close()
        }
    }
}