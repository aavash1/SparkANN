package com.aavash.ann.sparkann;

import java.awt.List;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
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

		$eq$colon$eq<String, String> tpEquals = scala.Predef.$eq$colon$eq$.MODULE$.tpEquals();
		// Load an external Text File in Apache spark
		JavaRDD<String> inputEdgesTextFile = javaSparkContext.textFile("//RoadNetworkDataset//SFEdges.txt");
		JavaRDD<String> inputNodesTextFile = javaSparkContext.textFile("//RoadNetworkDataset//SFNodes.txt");

		// Creating a RDD from textFile
		JavaRDD<EdgeNetwork> edgesPart = inputEdgesTextFile.mapPartitions(p -> {
			ArrayList<EdgeNetwork> edgeList = new ArrayList<EdgeNetwork>();
			while (p.hasNext()) {
				String[] parts = p.next().split(" ");
				EdgeNetwork edgeNet = new EdgeNetwork();
				edgeNet.setEdge_id(Integer.parseInt(parts[0]));
				edgeNet.setSource_id(Integer.parseInt(parts[1]));
				edgeNet.setDestination_id(Integer.parseInt(parts[2]));
				edgeNet.setEdge_length(Double.parseDouble(parts[3]));
				edgeList.add(edgeNet);

			}
			return edgeList.iterator();
		});

		JavaRDD<Node> nodesPart = inputNodesTextFile.mapPartitions(p -> {
			ArrayList<Node> nodeList = new ArrayList<Node>();
			while (p.hasNext()) {
				String[] parts = p.next().split(" ");
				Node node = new Node();

			}
			return nodeList.iterator();
		});

		edgesPart.foreach(edgeInfo -> System.out.println(
				"Edge_ID:" + edgeInfo.getEdge_id() + " " + "Source: " + edgeInfo.getSource_id() + " " + "Destination: "
						+ edgeInfo.getDestination_id() + " " + "EdgeLength: " + edgeInfo.getEdge_length()));

		JavaRDD<EdgeNetwork> edgesRDD = javaSparkContext.parallelize((java.util.List<EdgeNetwork>) edgesPart);
//		Graph<String, Integer> graph = Graph.fromEdges(edgesRDD.rdd(), "", StorageLevel.MEMORY_ONLY(),
	//			StorageLevel.MEMORY_ONLY(), stringTag, stringTag);

	}

}
