
package com.aavash.ann.sparkann;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.google.common.collect.LinkedHashMultimap;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.graphx.Edge;
import org.apache.spark.graphx.Graph;
import org.apache.spark.graphx.VertexRDD;
import org.apache.spark.storage.StorageLevel;

import com.aavash.ann.sparkann.graph.CustomPartitioner;
import com.ann.sparkann.framework.CoreGraph;
import com.ann.sparkann.framework.Node;
import com.ann.sparkann.framework.UtilitiesMgmt;
import com.ann.sparkann.framework.UtilsManagement;

import com.aavash.ann.sparkann.graph.Vertices;
import org.apache.spark.graphx.Edge;

import scala.Tuple2;
import scala.reflect.ClassTag;

public class METISReader implements Serializable {

	@SuppressWarnings("unchecked")
	public static <T> void main(String[] args) throws IOException, IOException {

		ClassTag<String> stringTag = scala.reflect.ClassTag$.MODULE$.apply(String.class);
		ClassTag<Integer> intTag = scala.reflect.ClassTag$.MODULE$.apply(Integer.class);
		ClassTag<Double> doubleTag = scala.reflect.ClassTag$.MODULE$.apply(Double.class);
		ClassTag<Node> nodeTag = scala.reflect.ClassTag$.MODULE$.apply(Node.class);
		ClassTag<Vertices> vertexTag = scala.reflect.ClassTag$.MODULE$.apply(Vertices.class);

		Logger.getLogger("org.apache").setLevel(Level.WARN);

		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("Graph");
		SparkConf conf1 = new SparkConf().setMaster("local").setAppName("Graph")
				.set("spark.shuffle.service.enabled", "false").set("spark.driver.blockManager.port", "10026")
				.set("spark.driver.port", "10027").set("spark.cores.max", "3").set("spark.executor.memory", "1G")
				.set("spark.driver.host", "210.107.197.209").set("spark.shuffle.service.enabled", "false")
				.set("spark.dynamicAllocation.enabled", "false").set("spark.shuffle.blockTransferService", "nio");
		try (JavaSparkContext javaSparkContext = new JavaSparkContext(conf)) {
			// SparkANN/METISGraph/MetisTinyGraph
			// String metisInputGraph = "Metisgraph/metis.txt";
			// String metisPartitionOutputFile = "PartitionDataset/metis.txt";
			String metisInputGraph = "Metisgraph/Tinygraph.txt";
			String metisPartitionOutputFile = "PartitionDataset/tg_part.txt";

			// To load the Graph actually
			String nodeDatasetFile = "Dataset/TinygraphNodes.txt";
			String edgeDataSetFile = "Dataset/TinygraphEdges.txt";
			CoreGraph inputGraph = UtilsManagement.readEdgeFileReturnGraph(edgeDataSetFile);

			ArrayList<Node> inputGraphNodesInfo = UtilsManagement.readNodeFile(nodeDatasetFile);
			inputGraph.setNodesWithInfo(inputGraphNodesInfo);

			// inputGraph.printNodesInfo();

			// inputGraph.printEdgesInfo();
			// System.out.println("NodeId: " + inputGraph.getNode(2));

			// 1. Read METIS graph input
			Map<Integer, List<Integer>> metisGraph = new HashMap<Integer, List<Integer>>();
			metisGraph = UtilitiesMgmt.readMETISInputGraph(metisInputGraph, metisGraph);

			// 1.2 Creating RDD of the metisGraph
			List<Tuple2<Integer, List<Integer>>> mapMetisGraph = new ArrayList(metisGraph.size());
			for (Object key : metisGraph.keySet()) {
				List<Integer> neighboringNodes = new ArrayList<Integer>();
				for (int i = 0; i < metisGraph.get(key).size(); i++) {
					neighboringNodes.add(metisGraph.get(key).get(i));
				}
				mapMetisGraph.add(new Tuple2(key, neighboringNodes));
				// System.out.println(key + " " + metisHolder.get(key));

			}
			JavaPairRDD<Integer, List<Integer>> mapMetisGraphRDD = javaSparkContext.parallelizePairs(mapMetisGraph);
			// mapMetisGraphRDD.foreach(x -> System.out.println(x._1 + " " + x._2()));

			// 2. Read the output of METIS as partitionFile
			ArrayList<Integer> partitionIndex = new ArrayList<Integer>();
			partitionIndex = UtilitiesMgmt.readMETISPartition(metisPartitionOutputFile, partitionIndex);
			// 2.1 Create the RDD of the List.
			JavaRDD<Integer> mapMetisPartitionRDD = javaSparkContext.parallelize(partitionIndex);
			// mapMetisPartition.foreach(x -> System.out.println(x));

			// 3. Storing the partitionIndex and metisGraphInput in the same
			// LinkedHashMultiMap.
			LinkedHashMultimap<Integer, Map<Integer, List<Integer>>> metisGraphWithPartitionIndex = LinkedHashMultimap
					.create();
			for (int i = 0; i < partitionIndex.size(); i++) {
				Map<Integer, List<Integer>> nodesAndNeighbors = new HashMap<Integer, List<Integer>>();
				nodesAndNeighbors.put((Integer) metisGraph.keySet().toArray()[i],
						(List<Integer>) metisGraph.values().toArray()[i]);
				metisGraphWithPartitionIndex.put(partitionIndex.get(i), nodesAndNeighbors);
			}

			 System.out.println("The size of metisGraphWithPartition is: " +
			 metisGraphWithPartitionIndex);

			// 4. Creating an RDD of the metisGraphWithPartitionIndex.
			List<Tuple2<Integer, Map<Integer, List<Integer>>>> mapMetisGraphWithPartitionIndex = new ArrayList<>(
					metisGraphWithPartitionIndex.size());

			// 4.1 Converting the HashMap to scala.Tuple2 and adding object to the list.
			for (Map.Entry<Integer, Map<Integer, List<Integer>>> i : metisGraphWithPartitionIndex.entries()) {
				mapMetisGraphWithPartitionIndex
						.add(new Tuple2<Integer, Map<Integer, List<Integer>>>(i.getKey(), i.getValue()));
			}

			// 4.2 Creating a JavaPairRDD.
			JavaPairRDD<Integer, Map<Integer, List<Integer>>> metisGraphWithPartitionIndexRDD = javaSparkContext
					.parallelizePairs(mapMetisGraphWithPartitionIndex);

			 metisGraphWithPartitionIndexRDD.foreach(x -> System.out.println(x._1() + " "
			 + x._2()));

			JavaPairRDD<Integer, Map<Integer, List<Integer>>> customPartitioned = metisGraphWithPartitionIndexRDD
					.partitionBy(new CustomPartitioner(2));

			JavaRDD<Integer> result = customPartitioned.mapPartitionsWithIndex((idx, i) -> {
				List<Integer> partitionCheckList = new ArrayList<>();
				while (i.hasNext()) {
					partitionCheckList.add(i.next()._1);
				}
				return partitionCheckList.iterator();
			}, true);

			 System.out.println(result.collect());

			 System.out.println("Num partitions " + result.getNumPartitions());

			// Create Graph using graphX
			List<Edge<Double>> edges = new ArrayList<>();
			List<Tuple2<Object, Vertices>> vertex = new ArrayList<>();

			UtilitiesMgmt.readTextEdgeFile(edges, edgeDataSetFile);
			List<Tuple2<Object, Vertices>> vertices = UtilitiesMgmt.readTextNodeFile(vertex, nodeDatasetFile);

			JavaRDD<Edge<Double>> edgeRDD = javaSparkContext.parallelize(edges);
			JavaRDD<Tuple2<Object, Vertices>> nodeRDD = javaSparkContext.parallelize(vertices);

			Graph<Vertices, Double> graph = Graph.apply(nodeRDD.rdd(), edgeRDD.rdd(), new Vertices(),
					StorageLevel.MEMORY_ONLY(), StorageLevel.MEMORY_ONLY(), vertexTag, doubleTag);

			graph.vertices().toJavaRDD().collect().forEach(System.out::println);
			graph.edges().toJavaRDD().collect().forEach(System.out::println);

//			VertexRDD<Vertices> output = graph.vertices();
//			JavaRDD<Tuple2<Object, Vertices>> output_rdd = output.toJavaRDD();
//			Tuple2<Object, Vertices> max_val = output_rdd.first();
//			System.out.println(max_val._1 + " has " + max_val._2());

		}

	}

}
