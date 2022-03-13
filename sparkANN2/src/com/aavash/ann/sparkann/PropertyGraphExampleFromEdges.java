package com.aavash.ann.sparkann;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.graphx.Edge;
import org.apache.spark.graphx.Graph;

import org.apache.spark.graphx.PartitionStrategy;
import org.apache.spark.storage.StorageLevel;

import com.aavash.ann.sparkann.graph.Utilsmanagement;

import scala.Tuple2;
import scala.reflect.ClassTag;

public class PropertyGraphExampleFromEdges {
	public static <T> void main(String[] args) throws IOException {

		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("Graph");
//
//		SparkConf conf = new SparkConf().setMaster("spark://210.107.197.209:7077").setAppName("graph")
//				.set("spark.blockManager.port", "10025").set("spark.driver.blockManager.port", "10026")
//				.set("spark.driver.port", "10027").set("spark.cores.max", "12").set("spark.executor.memory", "4g")
//				.set("spark.driver.host", "210.107.197.209").set("spark.shuffle.service.enabled", "false")
//				.set("spark.dynamicAllocation.enabled", "false");
		@SuppressWarnings("resource")
		JavaSparkContext javaSparkContext = new JavaSparkContext(conf);
		// JavaRDD<String> inputEdgesTextFile =
		// javaSparkContext.textFile("Dataset/SFEdge.txt");

		ClassTag<String> stringTag = scala.reflect.ClassTag$.MODULE$.apply(String.class);
		@SuppressWarnings("unused")
		ClassTag<Integer> intTag = scala.reflect.ClassTag$.MODULE$.apply(Integer.class);
		ClassTag<Double> doubleTag = scala.reflect.ClassTag$.MODULE$.apply(Double.class);

		List<Edge<Double>> edges = new ArrayList<>();
		List<Tuple2<Object, String>> nodes = new ArrayList<>();
		Map<Integer, Integer> toPartition = new HashMap<Integer, Integer>();

		String edgesInputFileName = "Dataset/SFEdge.txt";
		String nodesInputFileName = "Dataset/SFNodes.txt";
		// String partitionInputFile = "PartitionDataset/Cal_Part_2.txt";
		String partitionInputFile = "PartitionDataset/tg_part.txt";

		String metisInputGraph = "Metisgraph/Tinygraph.txt";
		HashMap<Object, ArrayList<Integer>> metisHolder = new HashMap<Object, ArrayList<Integer>>();

		// Edge datset contains edgeId|SourceId|DestinationId|EdgeLength
		// edges.add(new Edge<Double>(1, 2, 3.5));
		// edges.add(new Edge<Double>(2, 3, 4.8));
		// edges.add(new Edge<Double>(1, 3, 6.5));
		// edges.add(new Edge<Double>(4, 3, 1.8));
		// edges.add(new Edge<Double>(4, 5, 9.6));
		// edges.add(new Edge<Double>(2, 5, 3.3));

		Utilsmanagement.readTextEdgeFile(edges, edgesInputFileName);
		Utilsmanagement.readTextNodeFile(nodes, nodesInputFileName);
		metisHolder = Utilsmanagement.readMETISInputGraph(metisInputGraph, metisHolder);

		JavaRDD<Edge<Double>> edgeRDD = javaSparkContext.parallelize(edges);
		JavaRDD<Tuple2<Object, String>> nodeRDD = javaSparkContext.parallelize(nodes);
//		JavaRDD<Map<Integer, Integer>> toPartitonRDD = javaSparkContext
//				.parallelize((List<Map<Integer, Integer>>) toPartition);

		Graph<String, Double> graph = Graph.apply(nodeRDD.rdd(), edgeRDD.rdd(), "", StorageLevel.MEMORY_ONLY(),
				StorageLevel.MEMORY_ONLY(), stringTag, doubleTag);

		// Graph<String, Double> graph = Graph.fromEdges(edgeRDD.rdd(), "",
		// StorageLevel.MEMORY_ONLY(),
		// StorageLevel.MEMORY_ONLY(), stringTag, doubleTag);

		// graph.edges().toJavaRDD().collect().forEach(System.out::println);
		// graph.vertices().toJavaRDD().collect().forEach(System.out::println);
		graph.edges().toJavaRDD().collect();
//
//		 graph.edges().toJavaRDD().foreach(x -> System.out.println("SourceNode: " +
//		 x.srcId() + " , DestinationNode: "
//		 + x.dstId() + ", Distance SRC-DEST: " + x.attr$mcD$sp()));

		// graph.partitionBy(PartitionStrategy.RandomVertexCut$.MODULE$, 3);
//		Graph<Object, Double> connectedComponents = graph.ops().connectedComponents();
//		connectedComponents.vertices().toJavaRDD().collect().forEach(System.out::println);
		// triangleCount.vertices().toJavaRDD().collect().forEach(System.out::println);

	}

}