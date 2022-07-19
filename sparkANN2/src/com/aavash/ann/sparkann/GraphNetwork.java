package com.aavash.ann.sparkann;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Vector;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.graphx.Edge;
import org.apache.spark.graphx.Graph;
import org.apache.spark.graphx.PartitionStrategy;
import org.apache.spark.internal.config.R;
import org.apache.spark.storage.StorageLevel;
import com.aavash.ann.sparkann.algorithm.ANNNaive;
import com.aavash.ann.sparkann.algorithm.RandomObjectGenerator;
import com.aavash.ann.sparkann.graph.CustomPartitioner;
import com.ann.sparkann.framework.CoreGraph;
import com.ann.sparkann.framework.Node;
import com.ann.sparkann.framework.RoadObject;
import com.ann.sparkann.framework.UtilitiesMgmt;
import com.ann.sparkann.framework.UtilsManagement;
import com.ann.sparkann.framework.cEdge;
import com.google.common.collect.LinkedHashMultimap;

import io.netty.util.internal.PriorityQueue;
import scala.Tuple2;
import scala.reflect.ClassTag;

public class GraphNetwork {

	public static void main(String[] args) throws IOException {

		// Defining tags

		ClassTag<Double> doubleTag = scala.reflect.ClassTag$.MODULE$.apply(Double.class);
		ClassTag<Node> nodeTag = scala.reflect.ClassTag$.MODULE$.apply(Node.class);

		/**
		 * 1 Pass the path for loading the datasets 1.1 Dataset for graph containing
		 * nodes and edges
		 */
		String nodeDatasetFile = "Dataset/PCManualGraphNodes.txt";
		String edgeDataSetFile = "Dataset/PCManualGraphEdges.txt";

		/**
		 * 1.2 Dataset for METIS graph and Partition Output
		 */
		String metisInputGraph = "Metisgraph/ManualGraph.txt";
		String metisPartitionOutputFile = "PartitionDataset/PCmanualGr_part2.txt";

		/**
		 * Load Graph using CoreGraph Framework
		 */
		CoreGraph cGraph = UtilsManagement.readEdgeTxtFileReturnGraph(edgeDataSetFile);

		/**
		 * Create Vertices List from the nodeDataset
		 */
		ArrayList<Node> nodesList = UtilsManagement.readTxtNodeFile(nodeDatasetFile);
		cGraph.setNodesWithInfo(nodesList);
		// cGraph.printEdgesInfo();

		/**
		 * Generate Random Objects on Edge Data Object=100 Query Object=500 Manual Road
		 * Object is also used for testing
		 */
		// RandomObjectGenerator.generateUniformRandomObjectsOnMap(cGraph, 100, 500);
		String PCManualObject = "Dataset/manualobject/ManualObjectsOnRoad.txt";
		UtilsManagement.readRoadObjectTxtFile1(cGraph, PCManualObject);
		cGraph.printObjectsOnEdges();

		/**
		 * Load Spark Necessary Items
		 */
		Logger.getLogger("org.apache").setLevel(Level.WARN);

		/**
		 * This is for testing the paths between two vertex
		 */
		int N = cGraph.getNodesWithInfo().size();
		int M = cGraph.getAdjancencyMap().size();

		int sourceVertex = 1;
		int destinationVertex = 12;

		int k = 2;

		kthLargestPathUtil(cGraph, N, M, sourceVertex, destinationVertex, k);
		/**
		 * Actual Spark thing opens here
		 */

		SparkConf config = new SparkConf().setMaster("local[*]").setAppName("Final Graph");

		try (JavaSparkContext jscontext = new JavaSparkContext(config)) {
			Long counter = 1L;
			int nodecounter = 1;
			List<Tuple2<Object, Node>> nodeList = new ArrayList<>();

			for (Node n : cGraph.getNodesWithInfo()) {
				nodeList.add(new Tuple2<>(counter, new Node(n.getNodeId(), n.getLongitude(), n.getLatitude())));
				counter++;
			}

			List<Edge<Double>> connectingEdges = new ArrayList<>();
			for (Integer src : cGraph.getAdjancencyMap().keySet()) {
				for (Integer dest : cGraph.getAdjancencyMap().get(src).keySet()) {
					connectingEdges.add(new Edge<>(src, dest, cGraph.getEdgeDistance(src, dest)));

				}

			}

			/**
			 * Create a JavaRDD for nodeList and Edges
			 */
			JavaRDD<Tuple2<Object, Node>> nodesRDD = jscontext.parallelize(nodeList);
			JavaRDD<Edge<Double>> edgesRDD = jscontext.parallelize(connectingEdges);

			/**
			 * System.out.println("Create a graph using the RDDs'");
			 */
			Graph<Node, Double> graph = Graph
					.apply(nodesRDD.rdd(), edgesRDD.rdd(), new Node(), StorageLevel.MEMORY_ONLY(),
							StorageLevel.MEMORY_ONLY(), nodeTag, doubleTag)
					.partitionBy(PartitionStrategy.EdgePartition1D$.MODULE$, 3);

			// graph.vertices().toJavaRDD().collect().forEach(System.out::println);

			/**
			 * Read the output of METIS as partitionFile
			 */
			ArrayList<Integer> graphPartitionIndex = new ArrayList<Integer>();
			graphPartitionIndex = UtilitiesMgmt.readMETISPartition(metisPartitionOutputFile, graphPartitionIndex);

			int[] keys = new int[graphPartitionIndex.size()];
			for (int i = 0; i < cGraph.getAdjancencyMap().size(); i++) {

				keys[i] = (int) cGraph.getAdjancencyMap().keySet().toArray()[i];

			}

			List<Tuple2<Object, Map<Object, Map<Object, Double>>>> adjacencyListWithPartitionIndex = new ArrayList<>(
					graphPartitionIndex.size());

			LinkedHashMultimap<Object, Object> partitionIndexWithVertexId = LinkedHashMultimap.create();
			Map<Object, Object> vertexIdPartitionIndex = new HashMap<Object, Object>();

			for (int i = 0; i < graphPartitionIndex.size(); i++) {
				Map<Object, Map<Object, Double>> mapForAdjacentEdges = new HashMap<Object, Map<Object, Double>>();
				Map<Object, Double> destinationEdges = new HashMap<Object, Double>();

				for (Integer dstIndex : cGraph.getAdjancencyMap().get(keys[i]).keySet()) {
					destinationEdges.put(Long.valueOf(dstIndex), cGraph.getAdjancencyMap().get(keys[i]).get(dstIndex));

				}

				partitionIndexWithVertexId.put(Long.valueOf(graphPartitionIndex.get(i)), keys[i]);

				vertexIdPartitionIndex.put(keys[i], Long.valueOf(graphPartitionIndex.get(i)));

				mapForAdjacentEdges.put(keys[i], destinationEdges);
				adjacencyListWithPartitionIndex.add(new Tuple2<Object, Map<Object, Map<Object, Double>>>(
						Long.valueOf(graphPartitionIndex.get(i)), mapForAdjacentEdges));

			}

			/**
			 * Create a JavaPair Rdd of the adjacencyList
			 */
			JavaPairRDD<Object, Map<Object, Map<Object, Double>>> adjacencyListWithPartitionIndexRDD = jscontext
					.parallelizePairs(adjacencyListWithPartitionIndex).partitionBy(new CustomPartitioner(3));

			/**
			 * Partition the RDD using the key of the JavaPairRDD
			 */
			JavaPairRDD<Object, Map<Object, Map<Object, Double>>> customPartitionedadjacencyListWithPartitionIndexRDD = adjacencyListWithPartitionIndexRDD
					.partitionBy(new CustomPartitioner(3));
			// System.out.println("Partitions: " +
			// customPartitionedadjacencyListWithPartitionIndexRDD.partitions());

			JavaRDD<Integer> result = customPartitionedadjacencyListWithPartitionIndexRDD
					.mapPartitionsWithIndex((idx, i) -> {
						List<Integer> partitionCheckList = new ArrayList<>();
						while (i.hasNext()) {
							partitionCheckList.add(Integer.parseInt(String.valueOf(i.next()._1)));
						}
						return partitionCheckList.iterator();
					}, true);

			System.out.println();

			Map<Object, Object> BoundaryNodes = new HashMap<>();
			ArrayList<Object> BoundaryNodeList = new ArrayList<>();
			ArrayList<cEdge> BoundaryEdge = new ArrayList<>();
			// Map<Object,ArrayList<Object>> Boundaries=new HashMap<>();

			for (cEdge selectedEdge : cGraph.getEdgesWithInfo()) {
				int SrcId = selectedEdge.getStartNodeId();
				int DestId = selectedEdge.getEndNodeId();

				if (vertexIdPartitionIndex.get(SrcId) == vertexIdPartitionIndex.get(DestId)) {

				} else {

					BoundaryNodes.put(SrcId, vertexIdPartitionIndex.get(SrcId));
					BoundaryNodes.put(DestId, vertexIdPartitionIndex.get(DestId));
					BoundaryEdge.add(selectedEdge);

				}

			}

			for (Object BoundaryVertex : BoundaryNodes.keySet()) {
				BoundaryNodeList.add(BoundaryVertex);
			}

			// System.out.println(BoundaryNodes);
			// System.out.println(BoundaryEdge);

			JavaRDD<Object> BoundaryVertexRDD = jscontext.parallelize(BoundaryNodeList);
			JavaRDD<cEdge> BoundaryEdgeRDD = jscontext.parallelize(BoundaryEdge);

			// BoundaryVertexRDD.collect().forEach(x -> System.out.print(x + " "));
			// System.out.println(" ");
			// BoundaryEdgeRDD.collect().forEach(x -> System.out.print(x.getEdgeId() + "
			// "));
			// System.out.println(" ");

			List<Tuple2<Integer, ArrayList<RoadObject>>> roadObjectList = new ArrayList<>(
					cGraph.getObjectsOnEdges().size());

			for (Integer edgeId : cGraph.getObjectsOnEdges().keySet()) {
				roadObjectList.add(

						new Tuple2<Integer, ArrayList<RoadObject>>((Integer) edgeId,
								cGraph.getObjectsOnEdges().get(edgeId)));
			}
			JavaPairRDD<Integer, ArrayList<RoadObject>> roadObjectListRDD = jscontext.parallelizePairs(roadObjectList);
			// roadObjectListRDD.collect().forEach(System.out::println);

			/**
			 * Creating Embedded Network 1) Create a VIRTUAL NODE First with NodeId=maxvalue
			 * 2) Create a graph connecting VIRTUAL NODE to every other boundary Nodes 3)
			 * Set the weights as ZERO 4) Run the traversal from VIRTUAL NODE to other
			 * BOUNDARY NODES 5) Calcuate the distance to the nearest node and store it in a
			 * array Tuple2<Object,Map<Object,Double>> VirtualGraph
			 **/
			JavaPairRDD<Object, Map<Object, Double>> embeddedNetworkRDD = jscontext
					.parallelizePairs(createEmbeddedNetwork(BoundaryVertexRDD));
//			embeddedNetworkRDD.collect().forEach(
//					x -> System.out.print("Map: " + x + "\n" + " key: " + x._1 + " value: " + x._2 + "\n" + "\n"));

			// adjacencyListWithPartitionIndexRDD.collect().forEach(x -> System.out.print(x
			// + "\n"));

			/**
			 * Once the graph is created: 1) Combine the GraphRDD with RoadObjectPairRDD,
			 * and BoundaryEdgeRDD. 2) Apply CustomPartitioner function on the newly created
			 * RDD 3)
			 */

//			ANNNaive annNaive = new ANNNaive();
//			long startTimeNaive = System.nanoTime();
//			annNaive.compute(cGraph, true);
//			long timeElapsed = System.nanoTime() - startTimeNaive;
//			double computationTime = (double) timeElapsed / 1000000000.0;
//
//			System.out.print("The time to compute ANN: " + computationTime);

			jscontext.close();

		}

	}

	public static List<Tuple2<Object, Map<Object, Double>>> createEmbeddedNetwork(JavaRDD<Object> BoundaryVerticesRDD) {
		Object virtualVertex = Integer.MAX_VALUE;
		List<Tuple2<Object, Map<Object, Double>>> embeddedNetwork = new ArrayList<>();
		for (Object BoundaryVertex : BoundaryVerticesRDD.collect()) {
			Map<Object, Double> connectingVertex = new HashMap<Object, Double>();
			connectingVertex.put(BoundaryVertex, 0.0);
			embeddedNetwork.add(new Tuple2<Object, Map<Object, Double>>(virtualVertex, connectingVertex));

		}

		return embeddedNetwork;

	}

	public static void visitPaths(CoreGraph cg, Integer src, Integer dest) {

		boolean[] isVistied = new boolean[cg.getNodesWithInfo().size()];
		ArrayList<Integer> pathList = new ArrayList<>();

		pathList.add(src);

		printAllPathsUtil(cg, src, dest, isVistied, pathList);

	}

	private static void printAllPathsUtil(CoreGraph cg, Integer u, Integer d, boolean[] isVisited,
			List<Integer> localPathList) {

		if (u.equals(d)) {
			System.out.println(localPathList);
			return;
		}

		isVisited[u] = true;

		for (Integer i : cg.getAdjacencyEdgeIds(u)) {
			if (!isVisited[i]) {
				localPathList.add(i);
				printAllPathsUtil(cg, i, d, isVisited, localPathList);

				localPathList.remove(i);
			}

		}
		isVisited[u] = false;

	}

	class shortestPathBetweenBorderVertex implements Function<Map<Integer, Map<Integer, Double>>, R> {
		@Override
		public R call(Map<Integer, Map<Integer, Double>> cGraphMap) throws Exception {
			// TODO Auto-generated method stub
			return null;
		}
	};

	static class Pair implements Comparable<Pair> {
		// weight so far
		double wsf;

		// path so far
		String psf;

		Pair(double wsf, String psf) {
			// TODO Auto-generated constructor stub
			this.wsf = wsf;
			this.psf = psf;
		}

		@Override
		public int compareTo(Pair o) {
			return Double.compare(wsf, o.wsf);

		}
	}

	static Vector<Pair> pq = new Vector<Pair>();

	private static void kthLargest(CoreGraph graph, int src, int dest, boolean[] visited, int k, String psf,
			double wsf) {

		if (src == dest) {
			if (pq.size() < k) {
				pq.add(new Pair(wsf, psf));

			} else if (wsf > pq.indexOf(wsf)) {
				pq.remove(wsf);
				pq.add(new Pair(wsf, psf));
			}
			return;
		}

		visited[src] = true;

		for (cEdge e : graph.getEdgesWithInfo()) {

			if (!visited[e.getEndNodeId()]) {
				kthLargest(graph, e.getEndNodeId(), dest, visited, k, psf + e.getEndNodeId(), wsf + e.getLength());
			}

		}

		visited[src] = false;

	}

	// N= number of vertice, M= number of edges
	private static void kthLargestPathUtil(CoreGraph gr, int N, int M, int src, int dest, int k) {

		boolean[] visited = new boolean[gr.getNodesWithInfo().size()];

		kthLargest(gr, src, dest, visited, k, src + " ", 0);

		String path = pq.firstElement().psf;

		for (int i = 0; i < path.length(); i++) {
			System.out.print(path.charAt(i) + " ");
		}

	}

	/**
	 * ending bracket
	 * 
	 */
}