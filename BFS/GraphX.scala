import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
import org.apache.spark.storage.StorageLevel
import scala.language.implicitConversions
import scala.reflect.ClassTag
import org.apache.spark.graphx.impl._

abstract class GraphX[VD: ClassTag, ED: ClassTag] protected () extends Graph[VD, ED]{}

object GraphX {
  // Add a new edge
  def addEdge[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], edge: RDD[Edge[ED]], defaultValue: VD): Graph[VD, ED] = {
    val newEdges = graph.edges.union(edge).distinct()

    val p = PartitionStrategy.RandomVertexCut
    //Graph(graph.vertices, newEdges).partitionBy(p).groupEdges((a, b) => a + b)
    Graph(graph.vertices, newEdges)
  }

  // Adds a new Node to a graph
  def addNode[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], vertex: RDD[(VertexId, VD)]): Graph[VD, ED] = {
    Graph(vertex.union(graph.vertices), graph.edges)
  }

  def removeEdge[VD: ClassTag, ED: ClassTag](
  	graph: Graph[VD, ED], 
  	edge: RDD[Edge[ED]]): Graph[VD, ED] = {

  	val ed = edge.collect().head

    graph.subgraph(epred => {
      (epred.srcId != ed.srcId) || (epred.dstId != ed.dstId)
    })
  }

  def removeNode[VD: ClassTag, ED: ClassTag](
  	graph: Graph[VD, ED], 
  	vertex: RDD[(VertexId, VD)]): Graph[VD, ED] = {

  	var ve = vertex.collect().head._1

    graph.subgraph(vpred = (vId, name) => vId != ve)
  }

  // this will also accept the root or src id as input
  def shortestPath[VD: ClassTag, ED: ClassTag](
  	graph: Graph[VD, ED],
  	iter: Double, 
  	partialResult: Option[VertexRDD[(Double)]] = None): Graph[Double, ED] = {

    val root: VertexId = 1L

    val initialGraph: Graph[Double, ED] = partialResult match {
    	case Some(p) => graph.outerJoinVertices(p) {
    		(vid, data, par) => if(vid == root) 0.0 else par.getOrElse(Double.PositiveInfinity)
    	}
    	case None => graph.mapVertices((id, _) => if (id == root) 0.0 else Double.PositiveInfinity)
    }

    val iterated = initialGraph.mapVertices((id, data) => if(data != Double.PositiveInfinity) iter else data)

    val bfs = iterated.pregel(Double.PositiveInfinity, 20)( 
      (id, attr, msg) => math.min(attr, msg), triplet => { 
        if (triplet.srcAttr != Double.PositiveInfinity) { 
          Iterator((triplet.dstId, triplet.srcAttr + 1)) 
        }
        else { 
          Iterator.empty 
        } 
      }, (a, b) => math.min(a, b))

    bfs.outerJoinVertices(initialGraph.vertices) { 
    	(vid, data, par) => math.min(par.getOrElse(Double.PositiveInfinity), data)
    }

  }

  def shortestPathClock[VD: ClassTag, ED: ClassTag](
  	graph: Graph[VD, ED]): Graph[Double, ED] = {

    val root: VertexId = 1L

    val initialGraph: Graph[Double, ED] = graph.mapVertices((id, _) => if (id == root) 0.0 else Double.PositiveInfinity)

    val bfs = initialGraph.pregel(Double.PositiveInfinity, 20)( 
      (id, attr, msg) => math.min(attr, msg), triplet => { 
        if (triplet.srcAttr != Double.PositiveInfinity) { 
          var time = triplet.attr.asInstanceOf[(Long, Long)]
          if(triplet.srcAttr <= time._1.toDouble) {
          	Iterator((triplet.dstId, time._1.toDouble + 1))
          }
          else if(triplet.srcAttr <= time._2.toDouble) {
          	Iterator((triplet.dstId, triplet.srcAttr + 1))
          }
          else {
          	Iterator.empty
          }
        }
        else { 
          Iterator.empty 
        } 
      }, (a, b) => math.min(a, b))

    bfs
  }

  /**
   * Original methods for Graph object
   */

  def fromEdgeTuples[VD: ClassTag](
      rawEdges: RDD[(VertexId, VertexId)],
      defaultValue: VD,
      uniqueEdges: Option[PartitionStrategy] = None,
      edgeStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY,
      vertexStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY): Graph[VD, Int] =
  {
    val edges = rawEdges.map(p => Edge(p._1, p._2, 1))
    val graph = GraphImpl(edges, defaultValue, edgeStorageLevel, vertexStorageLevel)
    uniqueEdges match {
      case Some(p) => graph.partitionBy(p).groupEdges((a, b) => a + b)
      case None => graph
    }
  }

  def fromEdges[VD: ClassTag, ED: ClassTag](
      edges: RDD[Edge[ED]],
      defaultValue: VD,
      edgeStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY,
      vertexStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY): Graph[VD, ED] = {
    GraphImpl(edges, defaultValue, edgeStorageLevel, vertexStorageLevel)
  }

  def apply[VD: ClassTag, ED: ClassTag](
      vertices: RDD[(VertexId, VD)],
      edges: RDD[Edge[ED]],
      defaultVertexAttr: VD = null.asInstanceOf[VD],
      edgeStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY,
      vertexStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY): Graph[VD, ED] = {
    GraphImpl(vertices, edges, defaultVertexAttr, edgeStorageLevel, vertexStorageLevel)
  }

  implicit def graphToGraphOps[VD: ClassTag, ED: ClassTag]
      (g: Graph[VD, ED]): GraphOps[VD, ED] = g.ops
}