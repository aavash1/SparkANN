package com.aavash.ann.sparkann;

import java.awt.List;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.graphx.Edge;
import org.apache.spark.graphx.Graph;
import org.apache.spark.storage.StorageLevel;

import com.aavash.ann.sparkann.graph.EdgeNetwork;
import com.aavash.ann.sparkann.graph.Node;

import scala.Predef.$eq$colon$eq;
import scala.Tuple2;
import scala.reflect.ClassTag;

public class GraphDatasetParsingFile {
	public static void main(String[] args) {

		SparkConf conf = new SparkConf().setMaster("local").setAppName("GraphFileReadClass");
		JavaSparkContext javaSparkContext = new JavaSparkContext(conf);
		ClassTag<String> stringTag = scala.reflect.ClassTag$.MODULE$.apply(String.class);
		ClassTag<String> intTag = scala.reflect.ClassTag$.MODULE$.apply(Integer.class);
		ClassTag<Double> doubleTag = scala.reflect.ClassTag$.MODULE$.apply(Double.class);

		$eq$colon$eq<String, String> tpEquals = scala.Predef.$eq$colon$eq$.MODULE$.tpEquals();
		// Load an external Text File in Apache spark
		// The text files number of lines and each line consists these structure
		// SFEdge contains: | Edge_id integer | Source_Id integer | Destination_id
		// integer | EdgeLength double |
		// SFNodes contains: | Node_id integer | Longitude double | Latitude double |

		JavaRDD<String> inputEdgesTextFile = javaSparkContext.textFile("Dataset/SFEdge.txt");
		JavaRDD<String> inputNodesTextFile = javaSparkContext.textFile("Dataset/SFNodes.txt");
		ArrayList<Tuple2<Integer, Integer>> nodes = new ArrayList<>();
		ArrayList<Edge<Double>> edges = new ArrayList<>();

		JavaRDD<Node> nodesPart = inputNodesTextFile.mapPartitions(p -> {
			ArrayList<Node> nodeList = new ArrayList<Node>();
			int counter = 0;
			while (p.hasNext()) {
				String[] parts = p.next().split(" ");
				Node node = new Node();
				node.setNode_Id(Integer.parseInt(parts[0]));
				node.setLongitude(Double.parseDouble(parts[1]));
				node.setLatitude(Double.parseDouble(parts[2]));
				nodes.add(new Tuple2<Integer, Integer>(counter, Integer.parseInt(parts[0])));
				nodeList.add(node);
				counter++;

			}
			return nodeList.iterator();
		});
		JavaRDD<Tuple2<Integer, Integer>> nodesRDD = javaSparkContext.parallelize(nodes);
		nodesRDD.foreach(data -> System.out.print("Node details: " + data._1() + " " + data._2()));

		JavaRDD<EdgeNetwork> edgesPart = inputEdgesTextFile.mapPartitions(p -> {
			ArrayList<EdgeNetwork> edgeList = new ArrayList<EdgeNetwork>();
			while (p.hasNext()) {

				String[] parts = p.next().split(" ");
				EdgeNetwork edgeNet = new EdgeNetwork();
				edgeNet.setEdge_id(Integer.parseInt(parts[0]));
				edgeNet.setSource_id(Integer.parseInt(parts[1]));
				edgeNet.setDestination_id(Integer.parseInt(parts[2]));
				edgeNet.setEdge_length(Double.parseDouble(parts[3]));
				edges.add(new Edge<Double>(Long.parseLong(parts[1]), Long.parseLong(parts[2]),
						Double.parseDouble(parts[3])));
				edgeList.add(edgeNet);

			}
			return edgeList.iterator();
		});
		JavaRDD<Edge<Double>> edgesRDD = javaSparkContext.parallelize(edges);

		Graph<String, Double> graph = Graph.fromEdges(edgesRDD.rdd(), " ", StorageLevel.MEMORY_ONLY(),
				StorageLevel.MEMORY_ONLY(), stringTag, doubleTag);
		graph.vertices().toJavaRDD().collect().forEach(System.out::println);

	}
}
