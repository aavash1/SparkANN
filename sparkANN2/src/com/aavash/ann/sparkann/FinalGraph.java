package com.aavash.ann.sparkann;

import java.io.IOException;
import java.util.ArrayList;
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
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.graphx.Edge;
import org.apache.spark.graphx.Graph;
import org.apache.spark.storage.StorageLevel;

import com.aavash.ann.sparkann.graph.Vertices;
import com.ann.sparkann.framework.CoreGraph;
import com.ann.sparkann.framework.Node;
import com.ann.sparkann.framework.UtilitiesMgmt;
import com.ann.sparkann.framework.UtilsManagement;
import com.google.common.collect.LinkedHashMultimap;

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
		String nodeDatasetFile = "Dataset/TinygraphNodes.txt";
		String edgeDataSetFile = "Dataset/TinygraphEdges.txt";

		// 1.2 Dataset for METIS graph and Partition Output
		String metisInputGraph = "Metisgraph/Tinygraph.txt";
		String metisPartitionOutputFile = "PartitionDataset/tg_part.txt";

		// Load Graph using CoreGraph Framework
		CoreGraph cGraph = UtilsManagement.readEdgeTxtFileReturnGraph(edgeDataSetFile);

		// Create Vertices List from the nodeDataset
		ArrayList<Node> nodesList = UtilsManagement.readTxtNodeFile(nodeDatasetFile);
		cGraph.setNodesWithInfo(nodesList);

		// To validate logial graph is created
		// cGraph.printEdgesInfo();
		// cGraph.printNodesInfo();

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

			System.out.println("Create a graph using the RDDs'");
			Graph<Node, Double> graph = Graph.apply(nodesRDD.rdd(), edgesRDD.rdd(), new Node(),
					StorageLevel.MEMORY_ONLY(), StorageLevel.MEMORY_ONLY(), nodeTag, doubleTag);

			// graph.vertices().toJavaRDD().collect().forEach(System.out::println);

			// graph.edges().toJavaRDD().collect().forEach(System.out::println);

			// Read the output of METIS as partitionFile
			ArrayList<Integer> graphPartitionIndex = new ArrayList<Integer>();
			graphPartitionIndex = UtilitiesMgmt.readMETISPartition(metisPartitionOutputFile, graphPartitionIndex);

			LinkedHashMultimap<Integer, Map<Integer, Map<Integer, Double>>> cGraphWithPartitionIndex = LinkedHashMultimap
					.create();

			// System.out.println("Graph: " + cGraph.getAdjancencyMap());
			System.out.println();
//			int count = 0;
//			Map<Integer, Map<Integer, Double>> srcMap = new HashMap<Integer, Map<Integer, Double>>();
//
//			for (Integer in : cGraph.getAdjancencyMap().keySet()) {
//				// System.out.println("K1: " + in + " V in K1: " +
//				// cGraph.getAdjancencyMap().get(in).values());
//				Map<Integer, Double> destMap = new HashMap<Integer, Double>();
//
//				for (Integer in2 : cGraph.getAdjancencyMap().get(in).keySet()) {
//					// System.out.println("K2 in V: " + in2 + " V in K2: " +
//					// cGraph.getAdjancencyMap().get(in).get(in2));
//					destMap.put(in2, cGraph.getAdjancencyMap().get(in).get(in2));
//
//				}
//				srcMap.put(in, destMap);
////					cGraphWithPartitionIndex.put(partitionIndex.get(count), srcMap);
////					count++;
//				// System.out.println();
//			}

//			for (int i = 0; i < graphPartitionIndex.size(); i++) {
//				for (Entry<Integer, Map<Integer, Double>> entry : srcMap.entrySet()) {
//					cGraphWithPartitionIndex.put(graphPartitionIndex.get(0),
//							(Map<Integer, Map<Integer, Double>>) entry);
//					i++;
//					// System.out.print(entry);
//					// System.out.print(",");
//
//				}
//
//			}

//			for (int i = 0; i < graphPartitionIndex.size(); i++) {
//				Map<Integer, Map<Integer, Double>> srcMap = new HashMap<Integer, Map<Integer, Double>>();
//				for (Integer in : cGraph.getAdjancencyMap().keySet()) {
//					Map<Integer, Double> destMap = new HashMap<Integer, Double>();
//					for (Integer in2 : cGraph.getAdjancencyMap().get(in).keySet()) {
//						destMap.put(in2, cGraph.getAdjancencyMap().get(in).get(in2));
//					}
//					srcMap.put(in, destMap);
//					cGraphWithPartitionIndex.put(graphPartitionIndex.get(i), srcMap);
//					i++;
//					// System.out.println();
//				}
//
//			}

//			Set<Entry<Integer, Map<Integer, Double>>> set = cGraph.getAdjancencyMap().entrySet();
//			Map<Integer, Map<Integer, Double>> mapFromSet = new HashMap<Integer, Map<Integer, Double>>();
//			int count = 0;
//			for (Entry<Integer, Map<Integer, Double>> entry : set) {
//
//				mapFromSet.put(entry.getKey(), entry.getValue());
//				cGraphWithPartitionIndex.put(graphPartitionIndex.get(count), mapFromSet);
//
//			}
//
//			System.out.println(cGraphWithPartitionIndex);

			// List<Tuple2<PartitionIndex, Map< SourceNode, Map<destinationNode,Distance>>>.
			List<Tuple2<Object, Map<Object, Map<Object, Double>>>> adjacencyListWithPartitionIndex = new ArrayList<>(
					graphPartitionIndex.size());
			Set<Entry<Integer, Map<Integer, Double>>> set = cGraph.getAdjancencyMap().entrySet();
			Map<Object, Map<Object, Double>> mapFromSet = new HashMap<Object, Map<Object, Double>>();

			int count = 0;

			for (Entry<Integer, Map<Integer, Double>> entry : set) {
				Set<Entry<Integer, Double>> setWithinSet = entry.getValue().entrySet();
				Map<Object, Double> mapWithinMapFromSet = new HashMap<Object, Double>();
				for (Entry<Integer, Double> entryWithinSet : setWithinSet) {
					mapWithinMapFromSet.put(Long.valueOf(entryWithinSet.getKey()), entryWithinSet.getValue());
				}
				mapFromSet.put(entry.getKey(), mapWithinMapFromSet);

			}

			System.out.println("Graph adjacency List: " + mapFromSet);
			System.out.println("Partition Index: " + graphPartitionIndex);

			for (int i = 0; i < adjacencyListWithPartitionIndex.size(); i++) {
				adjacencyListWithPartitionIndex.add(new Tuple2<Object, Map<Object, Map<Object, Double>>>(
						Long.valueOf(graphPartitionIndex.get(i)), mapFromSet));
			}

			JavaPairRDD<Object, Map<Object, Map<Object, Double>>> adjacencyListWithPartitionIndexRDD = jscontext
					.parallelizePairs(adjacencyListWithPartitionIndex);

			adjacencyListWithPartitionIndexRDD.foreach(x -> System.out.println(x._1() + " " + x._2()));

			jscontext.close();
		}

	}

}
