package com.aavash.ann.sparkann;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.graphx.Edge;
import org.apache.spark.graphx.EdgeDirection;
import org.apache.spark.graphx.EdgeTriplet;
import org.apache.spark.graphx.Graph;
import org.apache.spark.graphx.GraphOps;
import org.apache.spark.graphx.PartitionStrategy;
import org.apache.spark.storage.StorageLevel;

import com.aavash.ann.pregel.VprogD;
import com.aavash.ann.pregel.mergeD;
import com.aavash.ann.pregel.sendMsgD;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import scala.Int;
import scala.Serializable;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.JavaConverters;
import scala.runtime.AbstractFunction1;
import scala.runtime.AbstractFunction2;
import scala.runtime.AbstractFunction3;

import com.ann.sparkann.framework.UtilitiesMgmt;

public class shortestPathDoubleWeight implements Serializable {

	private static final Object INITIAL_VALUE = Double.MAX_VALUE;
	private static final double DMAX_VAL = Double.MAX_VALUE;

	public static void main(String[] args) throws IOException {

		Logger.getLogger("org.apache.spark").setLevel(Level.WARN);

		SparkConf conf1 = new SparkConf().setMaster("local[*]").setAppName("ShortestPath");
		SparkConf conf = new SparkConf().setMaster("local").setAppName("Graph")
				.set("spark.shuffle.service.enabled", "false").set("spark.driver.blockManager.port", "10026")
				.set("spark.driver.port", "10027").set("spark.cores.max", "3").set("spark.executor.memory", "1G")
				.set("spark.driver.host", "210.107.197.209").set("spark.shuffle.service.enabled", "false")
				.set("spark.dynamicAllocation.enabled", "false").set("spark.shuffle.blockTransferService", "nio");
		JavaSparkContext jsp = new JavaSparkContext(conf);

		String nodeDatasetFile = "Dataset/TinygraphNodes.txt";
		String edgeDataSetFile = "Dataset/TinygraphEdges.txt";

		System.err.print("Create Labels Vertices and edges");
		Map<Long, Integer> labels = UtilitiesMgmt.readTextNodeReturnImtmap(nodeDatasetFile);
		List<Tuple2<Object, Double>> nodes = UtilitiesMgmt.getMapKeysCreateList(labels);
		List<Edge<Double>> edges = UtilitiesMgmt.readTextEdgeFileD(edgeDataSetFile);

		System.err.println("Create RDD for vertices and edges");
		JavaRDD<Tuple2<Object, Double>> nodesRDD = jsp.parallelize(nodes);
		JavaRDD<Edge<Double>> connectingEdgesRDD = jsp.parallelize(edges);

		Graph<Double, Double> graph = Graph.apply(nodesRDD.rdd(), connectingEdgesRDD.rdd(), 0.0,
				StorageLevel.MEMORY_ONLY(), StorageLevel.MEMORY_ONLY(),
				scala.reflect.ClassTag$.MODULE$.apply(Double.class),
				scala.reflect.ClassTag$.MODULE$.apply(Double.class));

		System.err.println("Create Graph from vertices and edges");
		graph.vertices().toJavaRDD().collect().forEach(System.out::println);
		graph.edges().toJavaRDD().collect().forEach(System.out::println);

		GraphOps ops = new GraphOps(graph, scala.reflect.ClassTag$.MODULE$.apply(Double.class),
				scala.reflect.ClassTag$.MODULE$.apply(Double.class));

		int srcLabel = labels.get(1l);

		System.err.println("Run pregel over our graph with apply, scatter and gather functions");
		System.out.println();

		JavaRDD<Tuple2<Object, Double>> output_rdd = ops.pregel(INITIAL_VALUE, Int.MaxValue(), EdgeDirection.Out(),
				new VprogD(), new sendMsgD(), new mergeD(), scala.reflect.ClassTag$.MODULE$.apply(Double.class))
				.vertices().toJavaRDD();

		output_rdd.sortBy(f -> ((Tuple2<Object, Double>) f)._1, true, 0).foreach(v -> {

			Tuple2<Object, Double> vertex = (Tuple2<Object, Double>) v;
			Long vertexId = (Long) vertex._1;
			Double cost = (Double) vertex._2;
			int descLabel = labels.get(vertexId);

			System.out.println("Minimum cost to get from '" + srcLabel + "' to '" + descLabel + "' is " + cost);
		});

	}

}
