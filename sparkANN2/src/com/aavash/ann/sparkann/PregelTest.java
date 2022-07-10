package com.aavash.ann.sparkann;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.graphx.Edge;
import org.apache.spark.graphx.Graph;
import org.apache.spark.graphx.PartitionStrategy;
import org.apache.spark.storage.StorageLevel;

//import com.aavash.ann.sparkann.graph.Node;

import scala.Tuple2;
import scala.Predef.$eq$colon$eq;
import scala.reflect.ClassTag;

public class PregelTest {

	public static void main(String[] args) {
		Logger.getLogger("org.apache.spark").setLevel(Level.WARN);

		SparkConf conf = new SparkConf().setMaster("local").setAppName("Pregel");
		JavaSparkContext javaSparkContext = new JavaSparkContext(conf);
		ClassTag<String> stringTag = scala.reflect.ClassTag$.MODULE$.apply(String.class);
		ClassTag<String> intTag = scala.reflect.ClassTag$.MODULE$.apply(Integer.class);
		ClassTag<Double> doubleTag = scala.reflect.ClassTag$.MODULE$.apply(Double.class);
		//ClassTag<Node> nodeTag = scala.reflect.ClassTag$.MODULE$.apply(Node.class);

		$eq$colon$eq<String, String> tpEquals = scala.Predef.$eq$colon$eq$.MODULE$.tpEquals();
		// Load an external Text File in Apache spark

		Double inf = Double.POSITIVE_INFINITY;

		List<Tuple2<Object, Double>> listOfNode = new ArrayList<>();
		listOfNode.add(new Tuple2<>(1L, 0.0));
		listOfNode.add(new Tuple2<>(2L, inf));
		listOfNode.add(new Tuple2<>(3L, inf));
		listOfNode.add(new Tuple2<>(4L, inf));
		JavaRDD<Tuple2<Object, Double>> nodeRDD = javaSparkContext.parallelize(listOfNode);

		List<Edge<Double>> listOfEdges = new ArrayList<>();
		listOfEdges.add(new Edge(1L, 2L, 5.0));
		listOfEdges.add(new Edge(1L, 3L, 7.0));
		listOfEdges.add(new Edge(2L, 4L, 12.0));
		listOfEdges.add(new Edge(3L, 4L, 8.0));

		JavaRDD<Edge<Double>> edgeRDD = javaSparkContext.parallelize(listOfEdges);

		Graph<Double, Double> graph = Graph.apply(nodeRDD.rdd(), edgeRDD.rdd(), 0.0, StorageLevel.MEMORY_ONLY(),
				StorageLevel.MEMORY_ONLY(), doubleTag, doubleTag)
				.partitionBy(PartitionStrategy.RandomVertexCut$.MODULE$);
		graph.vertices().toJavaRDD().collect().forEach(System.out::println);
		graph.edges().toJavaRDD().collect().forEach(System.out::println);

	}

}
