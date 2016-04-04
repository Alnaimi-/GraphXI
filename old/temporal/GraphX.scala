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

  def removeEdge[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], edge: RDD[Edge[ED]]): Graph[VD, ED] = {
  	val ed = edge.collect().head

    graph.subgraph(epred => {
      (epred.srcId != ed.srcId) || (epred.dstId != ed.dstId)
    })
  }

  def removeNode[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], vertex: RDD[(VertexId, VD)]): Graph[VD, ED] = {
  	var ve = vertex.collect().head._1

    graph.subgraph(vpred = (vId, name) => vId != ve)
  }

  def shortestPath[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], src: VertexId, timeInterval: (Long, Long)): List[(VertexId, Long)] = {
    /*
    * 1 foreach v ∈ V do 
    * 2 Create a sorted list for v, Lv, where an element of Lv is a pair
	*   (d[v], a[v]) in which d[v] is the distance of a path P from x to v
	*   and is used as the key for ordering in Lv, and a[v] is the time that
	*   the path P arrives at v; initially, Lv is empty;
    * 3 Initialize f[x] = 0, and f[v] = ∞ for all v ∈ V \ {x};
    * 4 foreach incoming edge e = (u, v, t, λ) in the edge stream do
    * 5 if t ≥ tα and t + λ ≤ tω then
    * 6 if u = x then
    * 7 if (0, t) ∈/ Lx then
    * 8 Insert (0, t) into Lx;
    * 9 Let (d'[u], a'[u]) be the element in Lu where a'[u] = max{a[u] : (d[u], a[u]) ∈ Lu, a[u] ≤ t};
    * 10 d[v] ← d'[u] + λ;
    * 11 a[v] ← t + λ;
    * 12 if a[v] is in Lv then
    * 13 Update the corresponding d[v] in Lv;
    * 14 else
    * 15 Insert (d[v], a[v]) into Lv;
    * 16 Remove dominated elements in Lv;
    * 17 if d[v] < f[v] then
    * 18 f[v] = d[v];
    * 19 else if t ≥ tω then
    * 20 Break the for-loop and go to Line 21;
    * 21 return f[v] for each v ∈ V ;
    */
	var map = graph.mapVertices[iLst[(Int, Long)]]((vid, data) => List[(Int, Long)]()).vertices.collectAsMap

	var shortest = graph.vertices.map(f => if (f._1 == src) (f._1, 0L) else (f._1, Long.MaxValue))

	graph.edges.sortBy(e => (e.attr.asInstanceOf[Long], e.srcId.asInstanceOf[Long])).collect.foreach(edge => {  	
	  var t = edge.attr.asInstanceOf[Long]
	  if(t >= timeInterval._1 && (t + 1) <= timeInterval._2) {
	  	if(edge.srcId == src) {
	      var lx = map.get(src).get
		  if(!lx.contains((0, edge.attr))) {
			(0, edge.attr) :: lx
			lx.sortBy(_._2)
		  }
		}

		/* 
		* Let (d'[u], a'[u]) be the element in Lu where a'[u] = max{a[u] : (d[u], a[u]) ∈ Lu, a[u] ≤ t};
		* d[v] ← d[u] + λ;
		* a[v] ← t + λ;
		*/
		var lu = map.get(edge.srcId).get
		// lu.find(elem => element._2 && elem._2 =< )

	  }
    })
    
    List[(VertexId, Long)]()

  }

  /* 
  * Input : A temporal graph G = (V, E) in its edge stream
  * representation, source vertex x, time interval [tα, tω]
  * Output : The earliest-arrival time from x to every vertex v ∈ V
  * within [tα, tω]
  * 1 Initialize t[x] = tα, and t[v] = ∞ for all v ∈ V \ {x};
  * 2 foreach incoming edge e = (u, v, t, λ) in the edge stream do
  * 3 if t + λ ≤ tω and t ≥ t[u] then
  * 4 if t + λ < t[v] then
  * 5 t[v] ← t + λ;
  * 6 else if t ≥ tω then
  * 7 Break the for-loop and go to Line 8;
  * 8 return t[v] for each v ∈ V ;
  */

  def earliestArrival[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], src: VertexId, timeInterval: (Long, Long)): List[(VertexId, Long)] = {
  	var earliest = graph.mapVertices[Long]((vid, data) => if (vid == src) timeInterval._1 else Long.MaxValue).vertices.collectAsMap

    graph.edges.sortBy(e => (e.attr.asInstanceOf[Long], e.srcId.asInstanceOf[Long])).collect.foreach(edge => {
      var t = edge.attr.asInstanceOf[Long]    
      if((t + 1L) <= timeInterval._2 && t >= earliest.get(edge.srcId).get) {
        if((t + 1L) < earliest.get(edge.dstId).get) {
          earliest = earliest.updated(edge.dstId, (t + 1L))
        }
      } 
      else if(t >= timeInterval._2)
        return earliest.toList
    })

    earliest.toList

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