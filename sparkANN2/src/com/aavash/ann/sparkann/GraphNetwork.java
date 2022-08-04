package com.aavash.ann.sparkann;

import java.io.IOException;

import java.util.ArrayList;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import java.util.Vector;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import org.apache.spark.graphx.Edge;
import org.apache.spark.graphx.Graph;
import org.apache.spark.graphx.PartitionStrategy;
import org.apache.spark.internal.config.R;
import org.apache.spark.storage.StorageLevel;
import org.apache.zookeeper.ZooDefs.Ids;

import com.aavash.ann.sparkann.graph.CustomPartitioner;
import com.ann.sparkann.framework.CoreGraph;
import com.ann.sparkann.framework.Node;
import com.ann.sparkann.framework.RoadObject;
import com.ann.sparkann.framework.UtilitiesMgmt;
import com.ann.sparkann.framework.UtilsManagement;
import com.ann.sparkann.framework.cEdge;
import com.google.common.collect.LinkedHashMultimap;

import scala.Function2;
import scala.Tuple2;
import scala.reflect.ClassTag;

import edu.ufl.cise.bsmock.graph.*;
import edu.ufl.cise.bsmock.graph.ksp.Yen;
import edu.ufl.cise.bsmock.graph.util.Path;

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
		 * Load Graph using CoreGraph Framework, YenGraph for calculating shortest paths
		 */
		CoreGraph cGraph = UtilsManagement.readEdgeTxtFileReturnGraph(edgeDataSetFile);
		YenGraph yGraph = new YenGraph(edgeDataSetFile);

		/**
		 * Create Vertices List from the nodeDataset
		 */
		ArrayList<Node> nodesList = UtilsManagement.readTxtNodeFile(nodeDatasetFile);
		cGraph.setNodesWithInfo(nodesList);
		// cGraph.printEdgesInfo();

		/**
		 * Test the YenGraph for finding SPF between two vertex
		 */

		/**
		 * Generate Random Objects on Edge Data Object=100 Query Object=500 Manual Road
		 * Object is also used for testing
		 */
		// RandomObjectGenerator.generateUniformRandomObjectsOnMap(cGraph, 100, 500);
		String PCManualObject = "Dataset/manualobject/ManualObjectsOnRoad.txt";
		UtilsManagement.readRoadObjectTxtFile1(cGraph, PCManualObject);
		// cGraph.printObjectsOnEdges();

		/**
		 * Load Spark Necessary Items
		 */
		Logger.getLogger("org.apache").setLevel(Level.WARN);

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
			 * Create a partition JavaPairRDD of the adjacencyList
			 */
			int CustomPartitionSize = 3;
			JavaPairRDD<Object, Map<Object, Map<Object, Double>>> custAdjListWithPartIndexRDD = jscontext
					.parallelizePairs(adjacencyListWithPartitionIndex)
					.partitionBy(new CustomPartitioner(CustomPartitionSize));
			System.out.println("Partitions: " + custAdjListWithPartIndexRDD.partitions().get(0));

			JavaRDD<Integer> result = custAdjListWithPartIndexRDD.mapPartitionsWithIndex((idx, i) -> {
				List<Integer> partitionCheckList = new ArrayList<>();
				while (i.hasNext()) {
					partitionCheckList.add(Integer.parseInt(String.valueOf(i.next()._1)));
				}
				return partitionCheckList.iterator();
			}, true);

			System.out.println();

			/**
			 * Selecting the Boundaries after graph partitions
			 * 
			 */

			Map<Object, Object> BoundaryNodes = new HashMap<>();
			LinkedList<String> BoundaryNodeList = new LinkedList<>();
			ArrayList<cEdge> BoundaryEdge = new ArrayList<>();

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

			/**
			 * This map holds partitionIndex as keys and ArrayList of Border vertices as
			 * values
			 **/

			Map<Object, ArrayList<Object>> Boundaries = new HashMap<>();
			LinkedHashMap<Integer, ArrayList<String>> strBoundaries = new LinkedHashMap<>();

			for (Object BoundaryVertex : BoundaryNodes.keySet()) {

				BoundaryNodeList.add(String.valueOf(BoundaryVertex));

				if (Boundaries.isEmpty()) {
					ArrayList<Object> vertices = new ArrayList<Object>();
					ArrayList<String> strVertices = new ArrayList<String>();
					vertices.add(BoundaryVertex);
					strVertices.add(String.valueOf(BoundaryVertex));
					Boundaries.put(BoundaryNodes.get(BoundaryVertex), vertices);
					strBoundaries.put(Integer.parseInt(String.valueOf(BoundaryNodes.get(BoundaryVertex))), strVertices);

				} else if (Boundaries.containsKey(BoundaryNodes.get(BoundaryVertex))) {
					Boundaries.get(BoundaryNodes.get(BoundaryVertex)).add(BoundaryVertex);
					strBoundaries.get(Integer.parseInt(String.valueOf(BoundaryNodes.get(BoundaryVertex))))
							.add(String.valueOf(BoundaryVertex));
				} else if (!(Boundaries.isEmpty()) && (!Boundaries.containsKey(BoundaryNodes.get(BoundaryVertex)))) {
					ArrayList<Object> vertices = new ArrayList<Object>();
					ArrayList<String> strVertices = new ArrayList<String>();
					vertices.add(BoundaryVertex);
					strVertices.add(String.valueOf(BoundaryVertex));
					Boundaries.put(BoundaryNodes.get(BoundaryVertex), vertices);
					strBoundaries.put(Integer.parseInt(String.valueOf(BoundaryNodes.get(BoundaryVertex))), strVertices);

				}
			}

			// System.out.println(BoundaryNodes);
			// System.out.println(BoundaryEdge);
			// System.out.println(strBoundaries);

			JavaRDD<String> BoundaryVertexRDD = jscontext.parallelize(BoundaryNodeList);
			JavaRDD<cEdge> BoundaryEdgeRDD = jscontext.parallelize(BoundaryEdge);

			BoundaryVertexRDD.collect().forEach(x -> System.out.print(x + " "));
			System.out.println(" ");
			// BoundaryEdgeRDD.collect().forEach(x -> System.out.print(x.getEdgeId() + "
			// "));
			// System.out.println(" ");

			List<Tuple2<Object, ArrayList<RoadObject>>> roadObjectList = new ArrayList<>(
					cGraph.getObjectsOnEdges().size());

			for (Integer edgeId : cGraph.getObjectsOnEdges().keySet()) {
				roadObjectList.add(

						new Tuple2<Object, ArrayList<RoadObject>>((Integer) edgeId,
								cGraph.getObjectsOnEdges().get(edgeId)));
			}
			JavaPairRDD<Object, ArrayList<RoadObject>> roadObjectListRDD = jscontext.parallelizePairs(roadObjectList);
			 roadObjectListRDD.collect().forEach(System.out::println);

			

			/**
			 * Using the Boundaries hashmap <PartitionIndex, ArrayList<BorderVertices> to
			 * find the shortest path from one partition to another partition Storing all
			 * the vertex that are in shortest path list in a separate ArrayList
			 */
			ArrayList<List<Path>> shortestPathList = runSPF(yGraph, strBoundaries, CustomPartitionSize);
			ArrayList<List<Path>> shortestPathList1 = runSP(yGraph, BoundaryNodeList, CustomPartitionSize);
//			System.out.println("Verify the shortest-paths");
//			for (List<Path> p1 : shortestPathList) {
//				System.out.println(p1 + " ");
//			}

			List<Integer> shortestpathsUnion = unifyAllShortestPaths(shortestPathList);
			System.out.println();
//			System.out.println(shortestpathsUnion);

//			List<Path> spflist = getSPFbetweenTwoNodes(yGraph, "38", "23", 2);

			/**
			 * Creating Embedded Network Graph: 1) Initially create a Virtual Vertex 2)
			 * Create a Embedded graph connecting VIRTUAL NODE to every other boundary Nodes
			 * 3) Set the weights as ZERO 4) Run the traversal from VIRTUAL NODE to other
			 * BOUNDARY NODES 5) Calculate the distance to the nearest node and store it in
			 * a array Tuple2<Object,Map<Object,Double>> VirtualGraph
			 **/

			for (List<Path> p1 : shortestPathList1) {
				for (Path p : p1) {
					// System.out.println("GetNodes: " + p.getNodes() + " ");
					System.out.println(p.getEdges().getFirst().getFromNode() + " To: "
							+ p.getEdges().getLast().getToNode() + " " + " Costt: " + p.getTotalCost() + " ");

				}
				// System.out.println(" ");

			}

			// CoreGraph spGraph = readSPFreturnGraph(shortestPathList);
			// System.out.println(" ");
			// cGraph.printEdgesInfo();
			// spGraph.printEdgesInfo();

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

	public static List<Tuple2<Object, Map<Object, Double>>> createEmbeddedNetwork(JavaRDD<String> BoundaryVerticesRDD) {
		Object virtualVertex = Integer.MAX_VALUE;
		List<Tuple2<Object, Map<Object, Double>>> embeddedNetwork = new ArrayList<>();
		for (Object BoundaryVertex : BoundaryVerticesRDD.collect()) {
			Map<Object, Double> connectingVertex = new HashMap<Object, Double>();
			connectingVertex.put(BoundaryVertex, 0.0);
			embeddedNetwork.add(new Tuple2<Object, Map<Object, Double>>(virtualVertex, connectingVertex));

		}

		return embeddedNetwork;

	}

	public static ArrayList<List<Path>> runSPF(YenGraph yenG, HashMap<Integer, ArrayList<String>> boundaries,
			int partSize) {

		ArrayList<List<Path>> SPList = new ArrayList<List<Path>>();
		Yen yenAlgo = new Yen();

		for (Integer mapIndex : boundaries.keySet()) {
			for (int i = 0; i < boundaries.get(mapIndex).size(); i++) {
				String vertexSrc = boundaries.get(mapIndex).get(i);

				for (int j = mapIndex + 1; j < partSize; j++) {
					for (int k = 0; k < boundaries.get(j).size(); k++) {
						String vertexDest = boundaries.get(j).get(k);
						List<Path> pathList = new LinkedList<Path>();
						pathList = yenAlgo.ksp(yenG, vertexSrc, vertexDest, 1);
						SPList.add(pathList);
						// 09System.out.println(vertexSrc + " -> " + vertexDest);

					}
				}

			}
		}

		return SPList;
	}

	public static ArrayList<List<Path>> runSP(YenGraph yenG, LinkedList<String> boundaryVertices, int partSize) {
		ArrayList<List<Path>> SPList = new ArrayList<List<Path>>();
		Yen yenAlgo = new Yen();

		for (int i = 0; i < boundaryVertices.size(); i++) {
			String srcVertex = boundaryVertices.get(i);
			for (int j = i + 1; j < boundaryVertices.size(); j++) {
				String destVertex = boundaryVertices.get(j);
				List<Path> pathList = new LinkedList<Path>();
				pathList = yenAlgo.ksp(yenG, srcVertex, destVertex, 1);
				SPList.add(pathList);
				// System.out.println(srcVertex + " , " + destVertex);
			}
		}
		return SPList;

	}

	public static List<Path> getSPFbetweenTwoNodes(YenGraph yg, String src, String dest, int partSize) {
		List<Path> spf = new ArrayList<Path>();
		Yen yalg = new Yen();

		spf = yalg.ksp(yg, src, dest, partSize);

		return spf;
	}

	public static LinkedList<Integer> unifyAllShortestPaths(ArrayList<List<Path>> spfList) {
		LinkedList<Integer> shortestpathsUnion = new LinkedList<Integer>();

		for (List<Path> p : spfList) {
			for (int i = 0; i < p.size(); i++) {
				for (int j = 0; j < p.get(i).getNodes().size(); j++) {
					int vertex = Integer.parseInt(p.get(i).getNodes().get(j));
					if (!shortestpathsUnion.contains(vertex)) {

						shortestpathsUnion.add(vertex);
						// System.out.println("added " + vertex);
					}

				}
				// System.out.println(" ");
			}
		}

		return shortestpathsUnion;
	}

	public static CoreGraph readSPFreturnGraph(ArrayList<List<Path>> shortestPathList) {

		CoreGraph embeddedGraph = new CoreGraph();

		for (List<Path> path : shortestPathList) {
			for (Path p : path) {
				int srcVertex = Integer.parseInt(p.getEdges().getFirst().getFromNode());
				int destinationVertex = Integer.parseInt(p.getEdges().getLast().getToNode());
				double edgeWeight = Double.valueOf(p.getTotalCost());
				embeddedGraph.addEdge(srcVertex, destinationVertex, edgeWeight);

			}
		}

		return embeddedGraph;

	}

	public static Tuple2<Integer, Integer> getNearestDataObject() {
		Tuple2<Integer, Integer> NNs = null;

		return NNs;
	}

	/**
	 * ending bracket
	 * 
	 */
}