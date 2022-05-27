package com.aavash.ann.sparkann;

import java.util.List;
import java.util.ArrayList;

import org.apache.commons.math3.geometry.spherical.twod.Vertex;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.Partition;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.graphx.Edge;
import org.apache.spark.graphx.EdgeDirection;
import org.apache.spark.graphx.Graph;
import org.apache.spark.graphx.GraphOps;
import org.apache.spark.graphx.PartitionStrategy;
import org.apache.spark.graphx.VertexRDD;
import org.apache.spark.storage.StorageLevel;

import com.aavash.ann.sparkann.graph.Vertices;

import scala.Predef.$eq$colon$eq;
import scala.Tuple2;
import scala.reflect.ClassTag;

public class GraphDatasetParsingFile2 {
	public static void main(String[] args) {

		Logger.getLogger("org.apache.spark").setLevel(Level.WARN);

		SparkConf conf = new SparkConf().setMaster("local").setAppName("GraphFileReadClass");
		JavaSparkContext javaSparkContext = new JavaSparkContext(conf);
		ClassTag<String> stringTag = scala.reflect.ClassTag$.MODULE$.apply(String.class);
		ClassTag<String> intTag = scala.reflect.ClassTag$.MODULE$.apply(Integer.class);
		ClassTag<Double> doubleTag = scala.reflect.ClassTag$.MODULE$.apply(Double.class);
		ClassTag<Vertices> nodeTag = scala.reflect.ClassTag$.MODULE$.apply(Vertices.class);

		$eq$colon$eq<String, String> tpEquals = scala.Predef.$eq$colon$eq$.MODULE$.tpEquals();
		// Load an external Text File in Apache spark
		// The text files number of lines and each line consists these structure
		// SFEdge contains: | Edge_id integer | Source_Id integer | Destination_id
		// integer | EdgeLength double |
		// SFNodes contains: | Node_id integer | Longitude double | Latitude double |

		JavaRDD<String> inputEdgesTextFile = javaSparkContext.textFile("Dataset/TinyGraphEdge.txt");
		JavaRDD<String> inputNodesTextFile = javaSparkContext.textFile("Dataset/TinyGraphNodes.txt");
		List<Tuple2<Object, Vertices>> listOfNode = new ArrayList<>();
		listOfNode.add(new Tuple2<>(1L, new Vertices(1, 2.0, 8.0)));
		listOfNode.add(new Tuple2<>(2L, new Vertices(2, 2.0, 3.0)));
		listOfNode.add(new Tuple2<>(3L, new Vertices(3, 5.0, 6.0)));
		listOfNode.add(new Tuple2<>(4L, new Vertices(4, 7.0, 2.0)));
		listOfNode.add(new Tuple2<>(5L, new Vertices(5, 5.0, 11.0)));
		listOfNode.add(new Tuple2<>(6L, new Vertices(6, 9.0, 8.0)));
		listOfNode.add(new Tuple2<>(7L, new Vertices(7, 10.0, 3.0)));

		JavaRDD<Tuple2<Object, Vertices>> verticesRDD = javaSparkContext.parallelize(listOfNode);

		List<Edge<Double>> edges = new ArrayList<>();
		edges.add(new Edge<>(1L, 2L, 5.0));
		edges.add(new Edge<>(1L, 3L, 7.0));
		edges.add(new Edge<>(2L, 4L, 12.0));
		edges.add(new Edge<>(3L, 4L, 8.0));

		JavaRDD<Edge<Double>> edgeRDD = javaSparkContext.parallelize(edges);

		Graph<Vertices, Double> graph = Graph
				.apply(verticesRDD.rdd(), edgeRDD.rdd(), new Vertices(), StorageLevel.MEMORY_ONLY(),
						StorageLevel.MEMORY_ONLY(), nodeTag, doubleTag)
				.partitionBy(PartitionStrategy.EdgePartition2D$.MODULE$, 2);
		

//		graph.vertices().toJavaRDD().collect().forEach(System.out::println);
//		System.out.println();
//		graph.edges().toJavaRDD().collect().forEach(System.out::println);
//		System.out.println();

//		GraphOps ops = new GraphOps(graph, nodeTag, doubleTag);
		VertexRDD<Vertices> output = graph.vertices();
		JavaRDD<Tuple2<Object,Vertices>> output_rdd=output.toJavaRDD();
		Tuple2<Object,Vertices> max_val=output_rdd.first();
		System.out.println(max_val._1+" has "+max_val._2());
		//System.out.println("Num of vertices: " + output.count());
		//Object[] parts=output.collectPartitions();
		

	}
}
