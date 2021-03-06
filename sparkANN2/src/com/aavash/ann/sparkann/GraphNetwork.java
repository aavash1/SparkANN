package com.aavash.ann.sparkann;

import java.io.IOException;

import java.util.ArrayList;

import java.util.HashMap;

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

import com.aavash.ann.sparkann.graph.CustomPartitioner;
import com.ann.sparkann.framework.CoreGraph;
import com.ann.sparkann.framework.Node;
import com.ann.sparkann.framework.RoadObject;
import com.ann.sparkann.framework.UtilitiesMgmt;
import com.ann.sparkann.framework.UtilsManagement;
import com.ann.sparkann.framework.cEdge;
import com.google.common.collect.LinkedHashMultimap;

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
		 * Load Graph using CoreGraph Framework
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
			ArrayList<String> BoundaryNodeList = new ArrayList<>();
			ArrayList<cEdge> BoundaryEdge = new ArrayList<>();

			// This map holds partitionIndex as keys and ArrayList of Border vertices as
			// values
			Map<Object, ArrayList<Object>> Boundaries = new HashMap<>();
			Map<Integer, ArrayList<String>> strBoundaries = new HashMap<>();
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
			System.out.println(strBoundaries);

			JavaRDD<String> BoundaryVertexRDD = jscontext.parallelize(BoundaryNodeList);
			JavaRDD<cEdge> BoundaryEdgeRDD = jscontext.parallelize(BoundaryEdge);

			BoundaryVertexRDD.collect().forEach(x -> System.out.print(x + " "));
			System.out.println(" ");
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
			 * Using the Boundaries hashmap <PartitionIndex, ArrayList<BorderVertices> to
			 * find the shortest path from one partition to anothe partition
			 */

			// ArrayList<List<Path>> shortestpathList = runSPF(yGraph, Boundaries);
//
//			int n = 0;
//			for (List<Path> p1 : shortestpathList) {
//				for (Path p : p1) {
//					System.out.println(++n + ")" + p);
//				}
//			}
//
//			for (int i = 0; i < BoundaryNodeList.size(); i++) {
//				String srcVertex = BoundaryNodeList.get(i);
//				for (int j = 1; j < strBoundaries.size(); j++) {
//					for (String popOut : strBoundaries.get(j)) {
//						String destVertex = popOut;
//						System.out.println(i + "-th iteration, the source is: " + srcVertex
//								+ ", and the destination is: " + destVertex);
//					}
//				}
//			}

			for (int i = 0; i < strBoundaries.size(); i++) {
				System.out.println("i's size: " + strBoundaries.get(i).size() + " i: " + strBoundaries.get(i));
				for (int j = 0; j < strBoundaries.get(i).size(); j++) {
					System.out.println("j: " + strBoundaries.get(i).get(j));
					

				}
			}

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

	public static ArrayList<List<Path>> runSPF(YenGraph yenG, Map<Object, ArrayList<Object>> boundaries) {

		ArrayList<List<Path>> SPList = new ArrayList<List<Path>>();
		Yen yenAlgo = new Yen();

		for (int i = 0; i < boundaries.keySet().size(); i++) {
			for (int j = 0; j < boundaries.get(i).size(); j++) {
				String srcVertex = (String) boundaries.get(i).get(j);
				for (int k = i + 1; k < boundaries.keySet().size(); k++) {
					for (int l = 0; l < boundaries.get(k).size(); k++) {
						String destVertex = (String) boundaries.get(k).get(l);
						List<Path> kshortestPathList = yenAlgo.ksp(yenG, srcVertex, destVertex, 1);
						SPList.add(kshortestPathList);
					}
				}
			}

		}
		return SPList;
	}

	/**
	 * ending bracket
	 * 
	 */
}