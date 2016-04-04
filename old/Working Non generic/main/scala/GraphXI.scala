import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream
import scala.collection.mutable

class GraphXI(val stream : DStream[String]) {
	// Retrieve our context(s) from the DStream
	var ssc : StreamingContext = stream.context
	var sc  : SparkContext     = ssc.sparkContext

	// Define an empty Graph object
	val users : RDD[(VertexId, String)] = sc.parallelize(Array.empty[(VertexId, String)])
	val edges : RDD[Edge[String]] = sc.parallelize(Array.empty[Edge[String]])
	var graph = Graph(users, edges)

	// Define a map of functions whose return
	// type is a Graph object
	var map = mutable.Map[String, Array[String] => Graph[String, String]]()
	map += {"addE"  -> addEdge}
	map += {"addN"  -> addNode}
	map += {"rmvE"  -> removeEdge}
	map += {"rmvN"  -> removeNode}
	// map += {"loadG" -> loadGraph}
	// map += {"viewG" -> viewGraph}  viewG yyyy-MM-dd HH:mm:ss

	def graphRecursion(spout : List[String]) {
		if (spout.isEmpty) return

	    val head = spout.head
	    val tail = spout.tail

	    val data = head.split(" ")

	    // Get operation command
	    val operation = data.head

   		try {
			val function = map.get(operation).get // .get to unwrap Some<function>
			graph = function(data.tail)           // sub-, or union-, graph of function

			graphRecursion(tail)
		} catch {
			case nse : NoSuchElementException =>
        	println("The method " + operation + " isn't valid.")
        	graphRecursion(tail)
    	}
	}

	// Constucts a new graph from Edges
	def addEdge(data : Array[String]) : Graph[String, String] = {
	    val srcId = data(0).toLong
	    val dstId = data(1).toLong
	    val msg = data(2)

	    val edge = sc.parallelize(Array(Edge(srcId, dstId, msg)))
	    val newEdges = edge.union(graph.edges)

			// TODO: Check for diff edges (msg included)
			//       check bi-directional

		val p = PartitionStrategy.RandomVertexCut
		Graph(graph.vertices, newEdges).partitionBy(p).groupEdges((a, b) => a + b)	
	}

	// Adds a new Node to a graph
	def addNode(data : Array[String]) : Graph[String, String] = {
	    val srcId = data(0).toLong
	    val name = data(1)

	    val user = sc.parallelize(Array((srcId, name)))
			Graph(user.union(graph.vertices), graph.edges)
	}

	def removeEdge(data : Array[String]) : Graph[String, String] = {
	    val srcId = data(0).toLong
	    val dstId = data(1).toLong

		graph.subgraph(edge => {
			(edge.srcId != srcId) || (edge.dstId != dstId)
		})
	}

	def removeNode(data : Array[String]) : Graph[String, String] = {
    	val srcId = data(0).toLong

		graph.subgraph(vpred = (vId, name) => vId != srcId)
	}
}