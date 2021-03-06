package com.aavash.ann.sparkann;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.graphx.Edge;
import org.apache.spark.graphx.Graph;
import org.apache.spark.graphx.PartitionStrategy;
import org.apache.spark.storage.StorageLevel;
import org.apache.zookeeper.Op.Create;

import com.aavash.ann.sparkann.algorithm.ANNNaive;
import com.aavash.ann.sparkann.algorithm.RandomObjectGenerator;
import com.aavash.ann.sparkann.graph.CustomPartitioner;
import com.aavash.ann.sparkann.graph.Vertices;
import com.ann.sparkann.framework.CoreGraph;
import com.ann.sparkann.framework.Node;
import com.ann.sparkann.framework.RoadObject;
import com.ann.sparkann.framework.UtilitiesMgmt;
import com.ann.sparkann.framework.UtilsManagement;
import com.ann.sparkann.framework.cEdge;
import com.google.common.collect.LinkedHashMultimap;
import com.google.errorprone.annotations.Immutable;

import avro.shaded.com.google.common.collect.ImmutableList;
import breeze.util.partition;
import scala.Tuple1;
import scala.Tuple2;
import scala.reflect.ClassTag;

public class FinalGraph {

	public static void main(String[] args) throws IOException {

		// Defining tags
		ClassTag<String> stringTag = scala.reflect.ClassTag$.MODULE$.apply(String.class);
		ClassTag<Integer> intTag = scala.reflect.ClassTag$.MODULE$.apply(Integer.class);
		ClassTag<Double> doubleTag = scala.reflect.ClassTag$.MODULE$.apply(Double.class);
		ClassTag<Node> nodeTag = scala.reflect.ClassTag$.MODULE$.apply(Node.class);
		ClassTag<Vertices> vertexTag = scala.reflect.ClassTag$.MODULE$.apply(Vertices.class);

		// Pass the path for loading the datasets
		// 1.1 Dataset for graph containing nodes and edges
		String nodeDatasetFile = "Dataset/ManualGraphNodes.txt";
		String edgeDataSetFile = "Dataset/ManualGraphEdges.txt";

		// 1.2 Dataset for METIS graph and Partition Output
		String metisInputGraph = "Metisgraph/ManualGraph.txt";
		String metisPartitionOutputFile = "PartitionDataset/manualGr_part.txt";

		// Load Graph using CoreGraph Framework
		CoreGraph cGraph = UtilsManagement.readEdgeTxtFileReturnGraph(edgeDataSetFile);

		// Create Vertices List from the nodeDataset
		ArrayList<Node> nodesList = UtilsManagement.readTxtNodeFile(nodeDatasetFile);
		cGraph.setNodesWithInfo(nodesList);

		// To validate logial graph is created
		 cGraph.printEdgesInfo();
		// cGraph.printNodesInfo();

		// Generate Random Objects on Edge
		RandomObjectGenerator.generateUniformRandomObjectsOnMap(cGraph, 100, 500);
		// Map<Integer, ArrayList<RoadObject>>
		for (Integer edgeId : cGraph.getObjectsOnEdges().keySet()) {
			System.out.println("EdgeID:" + edgeId + " has objects: " + cGraph.getObjectsOnEdges().get(edgeId) + ".");

		}

		// Load Spark Necessary Items
		Logger.getLogger("org.apache").setLevel(Level.WARN);

		SparkConf config = new SparkConf().setMaster("local[*]").setAppName("Final Graph");
		try (JavaSparkContext jscontext = new JavaSparkContext(config)) {
			// cGraph.printEdgesInfo();
			// cGraph.printNodesInfo();
			Long counter = 1L;
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

			// Validating that the nodelist and edges are created from core graph
//			System.out.println();
//			for (int i = 0; i < nodeList.size(); i++) {
//				System.out.println(nodeList.get(i));
//			}
//			System.out.println();
//			for (int i = 0; i < edges.size(); i++) {
//				System.out.println(edges.get(i));
//			}

			// Create a JavaRDD for nodeList and Edges
			JavaRDD<Tuple2<Object, Node>> nodesRDD = jscontext.parallelize(nodeList);
			JavaRDD<Edge<Double>> edgesRDD = jscontext.parallelize(connectingEdges);

			// System.out.println("Create a graph using the RDDs'");
			Graph<Node, Double> graph = Graph
					.apply(nodesRDD.rdd(), edgesRDD.rdd(), new Node(), StorageLevel.MEMORY_ONLY(),
							StorageLevel.MEMORY_ONLY(), nodeTag, doubleTag)
					.partitionBy(PartitionStrategy.EdgePartition1D$.MODULE$, 3);

			// graph.vertices().toJavaRDD().collect().forEach(System.out::println);

			// graph.edges().toJavaRDD().collect().forEach(System.out::println);

			// Read the output of METIS as partitionFile
			ArrayList<Integer> graphPartitionIndex = new ArrayList<Integer>();
			graphPartitionIndex = UtilitiesMgmt.readMETISPartition(metisPartitionOutputFile, graphPartitionIndex);

			// System.out.println("adj Map size: " + cGraph.getAdjancencyMap().size());
			int[] keys = new int[graphPartitionIndex.size()];
			for (int i = 0; i < cGraph.getAdjancencyMap().size(); i++) {

				keys[i] = (int) cGraph.getAdjancencyMap().keySet().toArray()[i];
				// System.out.println(keys[i]);
			}
//			for (Integer kInt : keys) {
//				System.out.println("Key: " + kInt + " Value: " + cGraph.getAdjancencyMap().get(kInt));
//			}

			List<Tuple2<Object, Map<Object, Map<Object, Double>>>> adjacencyListWithPartitionIndex = new ArrayList<>(
					graphPartitionIndex.size());
//			LinkedHashMultimap<Object, Map<Object, List<Object>>> adjacencyListWithPartIndex = LinkedHashMultimap
//					.create();
			LinkedHashMultimap<Object, Object> partitionIndexWithVertexId = LinkedHashMultimap.create();
			Map<Object, Object> vertexIdPartitionIndex = new HashMap<Object, Object>();

			for (int i = 0; i < graphPartitionIndex.size(); i++) {
//				Map<Object, List<Object>> adjacentVertexMap = new HashMap<Object, List<Object>>();
//				List<Object> destinationVertexList = new ArrayList<Object>();

				Map<Object, Map<Object, Double>> mapForAdjacentEdges = new HashMap<Object, Map<Object, Double>>();
				Map<Object, Double> destinationEdges = new HashMap<Object, Double>();

				for (Integer dstIndex : cGraph.getAdjancencyMap().get(keys[i]).keySet()) {
					destinationEdges.put(Long.valueOf(dstIndex), cGraph.getAdjancencyMap().get(keys[i]).get(dstIndex));

					// destinationVertexList.add(Long.valueOf(dstIndex));
				}
				// adjacentVertexMap.put(keys[i], destinationVertexList);
				// adjacencyListWithPartIndex.put(Long.valueOf(graphPartitionIndex.get(i)),
				// adjacentVertexMap);

				partitionIndexWithVertexId.put(Long.valueOf(graphPartitionIndex.get(i)), keys[i]);

				vertexIdPartitionIndex.put(keys[i], Long.valueOf(graphPartitionIndex.get(i)));

				mapForAdjacentEdges.put(keys[i], destinationEdges);
				adjacencyListWithPartitionIndex.add(new Tuple2<Object, Map<Object, Map<Object, Double>>>(
						Long.valueOf(graphPartitionIndex.get(i)), mapForAdjacentEdges));

			}

			// Create a JavaPair Rdd of the adjacencyList
			JavaPairRDD<Object, Map<Object, Map<Object, Double>>> adjacencyListWithPartitionIndexRDD = jscontext
					.parallelizePairs(adjacencyListWithPartitionIndex);

			// adjacencyListWithPartitionIndexRDD.foreach(x -> System.out.println(x._1() + "
			// " + x._2()));

			// Partition the RDD using the key of the JavaPairRDD
			JavaPairRDD<Object, Map<Object, Map<Object, Double>>> customPartitionedadjacencyListWithPartitionIndexRDD = adjacencyListWithPartitionIndexRDD
					.partitionBy(new CustomPartitioner(2));

			JavaRDD<Integer> result = customPartitionedadjacencyListWithPartitionIndexRDD
					.mapPartitionsWithIndex((idx, i) -> {
						List<Integer> partitionCheckList = new ArrayList<>();
						while (i.hasNext()) {
							partitionCheckList.add(Integer.parseInt(String.valueOf(i.next()._1)));
						}
						return partitionCheckList.iterator();
					}, true);

			System.out.println();
			// System.out.println(result.collect());

			// System.out.println("Num partitions " + result.getNumPartitions());

			Map<Object, Object> BoundaryNodes = new HashMap<>();
			ArrayList<cEdge> BoundaryEdge = new ArrayList<>();

			for (cEdge selectedEdge : cGraph.getEdgesWithInfo()) {
				int SrcId = selectedEdge.getStartNodeId();
				int DestId = selectedEdge.getEndNodeId();

				if (vertexIdPartitionIndex.get(SrcId) == vertexIdPartitionIndex.get(DestId)) {
					// System.out.println(SrcId + " and " + DestId + " Lies in same Partition");
				} else {
					// System.out.println(SrcId + " and " + DestId + " Lies in different
					// partitions");
					BoundaryNodes.put(SrcId, vertexIdPartitionIndex.get(SrcId));
					BoundaryNodes.put(DestId, vertexIdPartitionIndex.get(DestId));
					BoundaryEdge.add(selectedEdge);

				}

			}

			// System.out.println(BoundaryNodes);
			// System.out.println(BoundaryEdge);

			List<Tuple2<Integer, ArrayList<RoadObject>>> roadObjectList = new ArrayList<>(
					cGraph.getObjectsOnEdges().size());

			for (Integer edgeId : cGraph.getObjectsOnEdges().keySet()) {
				roadObjectList.add(
						new Tuple2<Integer, ArrayList<RoadObject>>(edgeId, cGraph.getObjectsOnEdges().get(edgeId)));
			}
			JavaPairRDD<Integer, ArrayList<RoadObject>> roadObjectListRDD = jscontext.parallelizePairs(roadObjectList);
			roadObjectListRDD.collect().forEach(System.out::println);

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

}
