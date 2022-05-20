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

import com.aavash.ann.pregel.Vprog;
import com.aavash.ann.pregel.merge;
import com.aavash.ann.pregel.sendMsg;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import scala.Serializable;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.JavaConverters;
import scala.runtime.AbstractFunction1;
import scala.runtime.AbstractFunction2;
import scala.runtime.AbstractFunction3;

import com.ann.sparkann.framework.UtilitiesMgmt;

public class shortestPath implements Serializable {

	private static final Object INITIAL_VALUE = Integer.MAX_VALUE;

	public static void main(String[] args) throws IOException {

		Logger.getLogger("org.apache.spark").setLevel(Level.WARN);

		SparkConf conf1 = new SparkConf().setMaster("local[*]").setAppName("ShortestPath");
		SparkConf conf = new SparkConf().setMaster("local").setAppName("Graph")
				.set("spark.shuffle.service.enabled", "false").set("spark.driver.blockManager.port", "10026")
				.set("spark.driver.port", "10027").set("spark.cores.max", "3").set("spark.executor.memory", "1G")
				.set("spark.driver.host", "210.107.197.209").set("spark.shuffle.service.enabled", "false")
				.set("spark.dynamicAllocation.enabled", "false").set("spark.shuffle.blockTransferService", "nio");
		JavaSparkContext jsp = new JavaSparkContext(conf);

		System.err.print("Create Labels Vertices and edges");
		Map<Long, String> labels = ImmutableMap.<Long, String>builder().put(1l, "A").put(2l, "B").put(3l, "C")
				.put(4l, "D").put(5l, "E").put(6l, "F").build();

		List<Tuple2<Object, Integer>> vertices = Lists.newArrayList(new Tuple2<Object, Integer>(1l, 0),
				new Tuple2<Object, Integer>(2l, Integer.MAX_VALUE), new Tuple2<Object, Integer>(3l, Integer.MAX_VALUE),
				new Tuple2<Object, Integer>(4l, Integer.MAX_VALUE), new Tuple2<Object, Integer>(5l, Integer.MAX_VALUE),
				new Tuple2<Object, Integer>(6l, Integer.MAX_VALUE));
		List<Edge<Integer>> edges = Lists.newArrayList(new Edge<Integer>(1l, 2l, 4), // A --> B (4)
				new Edge<Integer>(1l, 3l, 2), // A --> C (2)
				new Edge<Integer>(2l, 3l, 5), // B --> C (5)
				new Edge<Integer>(2l, 4l, 10), // B --> D (10)
				new Edge<Integer>(3l, 5l, 3), // C --> E (3)
				new Edge<Integer>(5l, 4l, 4), // E --> D (4)
				new Edge<Integer>(4l, 6l, 11) // D --> F (11)
		);

		System.err.println("Create RDD for vertices and edges");
		JavaRDD<Tuple2<Object, Integer>> verticesRDD = jsp.parallelize(vertices);
		JavaRDD<Edge<Integer>> edgesRDD = jsp.parallelize(edges);

		System.err.println("Create Graph from vertices and edges");
		Graph<Integer, Integer> G = Graph
				.apply(verticesRDD.rdd(), edgesRDD.rdd(), 1, StorageLevel.MEMORY_ONLY(), StorageLevel.MEMORY_ONLY(),
						scala.reflect.ClassTag$.MODULE$.apply(Integer.class),
						scala.reflect.ClassTag$.MODULE$.apply(Integer.class))
				.partitionBy(PartitionStrategy.RandomVertexCut$.MODULE$, 2);

		G.vertices().toJavaRDD().collect().forEach(System.out::println);
		G.edges().toJavaRDD().collect().forEach(System.out::println);

		GraphOps ops = new GraphOps(G, scala.reflect.ClassTag$.MODULE$.apply(Integer.class),
				scala.reflect.ClassTag$.MODULE$.apply(Integer.class));

		String srcLabel = labels.get(1l);

		System.err.println("Run pregel over our graph with apply, scatter and gather functions");
		System.err.println();

		JavaRDD<Tuple2<Object, Integer>> output_rdd = ops.pregel(INITIAL_VALUE, Integer.MAX_VALUE, EdgeDirection.Out(),
				new Vprog(), new sendMsg(), new merge(), scala.reflect.ClassTag$.MODULE$.apply(Integer.class))
				.vertices().toJavaRDD();

		System.err.println();

		output_rdd.sortBy(f -> ((Tuple2<Object, Integer>) f)._1, true, 0).foreach(v -> {

			Tuple2<Object, Integer> vertex = (Tuple2<Object, Integer>) v;
			Long vertexId = (Long) vertex._1;
			Integer cost = (Integer) vertex._2;
			String descLabel = labels.get(vertexId);

			System.out.println("Minimum cost to get from '" + srcLabel + "' to '" + descLabel + "' is " + cost);
		});

	}

}
