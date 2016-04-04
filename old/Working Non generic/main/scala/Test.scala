import org.apache.spark._
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.log4j.Logger
import org.apache.log4j.Level

object Test {
	val sparkConf = new SparkConf().setAppName("StreamX")
	
	val sc  = new SparkContext(sparkConf)
	val ssc = new StreamingContext(sc, Seconds(10))
	var graph = new GraphXI(stream)

	// Turn off the 100's of messages
	Logger.getLogger("org").setLevel(Level.OFF)
 	Logger.getLogger("akka").setLevel(Level.OFF)

	def main(args : Array[String]) {
		// Define a DStream
		var stream : DStream[String] = null

		args.length match {
			case 2 => stream = ssc.socketTextStream(args(0), args(1).toInt,	StorageLevel.MEMORY_AND_DISK_SER)
			case 1 => stream = ssc.textFileStream(args(0))
			case _ => println("Incorrect num of args, please refer to readme.md!")
		}

		readStream()

		ssc.start
		ssc.awaitTermination
	}

	def readStream() {
		stream.foreachRDD(rdd => {
			println("Starting new batch...")

			if(!rdd.isEmpty) {
        		val spout: List[String] = rdd.collect().toList

				graph.graphRecursion(spout)
			}

			printGraph()
		})
	}

	/*
		HELPER METHODS
	*/

	def printGraph() {
		println("Vertices:")
		graph.vertices.foreach(x => println(x))

		println("Edges:")
		println(graph.edges.map(_.copy()).distinct.count)
		graph.edges.foreach(x => println(x))
	}
}