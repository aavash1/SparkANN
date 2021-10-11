package com.aavash.ann.sparkann;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.graphx.Edge;
import org.apache.spark.graphx.Graph;
import org.apache.spark.graphx.GraphXUtils;
import org.apache.spark.graphx.PartitionStrategy;
import org.apache.spark.storage.StorageLevel;

import scala.Tuple2;
import scala.reflect.ClassTag;

public class PropertyGraphExampleFromEdges implements Serializable {
	public static void main(String[] args) throws IOException {
	
		SparkConf conf = new SparkConf().setMaster("local").setAppName("graph");
		JavaSparkContext javaSparkContext = new JavaSparkContext(conf);
		// JavaRDD<String> inputEdgesTextFile =
		// javaSparkContext.textFile("Dataset/SFEdge.txt");

		ClassTag<String> stringTag = scala.reflect.ClassTag$.MODULE$.apply(String.class);
		ClassTag<Integer> intTag = scala.reflect.ClassTag$.MODULE$.apply(Integer.class);
		ClassTag<Double> doubleTag = scala.reflect.ClassTag$.MODULE$.apply(Double.class);

		List<Edge<Double>> edges = new ArrayList<>();
		List<Tuple2<Object, String>> nodes = new ArrayList<>();

		String edgesInputFileName = "Dataset/SFEdge.txt";
		String nodesInputFileName = "Dataset/SFNodes.txt";

		// Edge datset contains edgeId|SourceId|DestinationId|EdgeLength
		// edges.add(new Edge<Double>(1, 2, 3.5));
		// edges.add(new Edge<Double>(2, 3, 4.8));
		// edges.add(new Edge<Double>(1, 3, 6.5));
		// edges.add(new Edge<Double>(4, 3, 1.8));
		// edges.add(new Edge<Double>(4, 5, 9.6));
		// edges.add(new Edge<Double>(2, 5, 3.3));

		readTextEdgeFile(edges, edgesInputFileName);
		readTextNodeFile(nodes, nodesInputFileName);

		JavaRDD<Edge<Double>> edgeRDD = javaSparkContext.parallelize(edges);
		JavaRDD<Tuple2<Object, String>> nodeRDD = javaSparkContext.parallelize(nodes);

		Graph<String, Double> graph = Graph.apply(nodeRDD.rdd(), edgeRDD.rdd(), "", StorageLevel.MEMORY_ONLY(),
				StorageLevel.MEMORY_ONLY(), stringTag, doubleTag);

		// Graph<String, Double> graph = Graph.fromEdges(edgeRDD.rdd(), "",
		// StorageLevel.MEMORY_ONLY(),
		// StorageLevel.MEMORY_ONLY(), stringTag, doubleTag);

		// graph.edges().toJavaRDD().collect().forEach(System.out::println);
		// graph.vertices().toJavaRDD().collect().forEach(System.out::println);

		// graph.edges().toJavaRDD().foreach(x -> System.out.println("SourceNode: " +
		// x.srcId() + " , DestinationNode: "
		// + x.dstId() + ", Distance SRC-DEST: " + x.attr$mcD$sp()));

		// Error is generated here below this comment
		graph.partitionBy(PartitionStrategy.CanonicalRandomVertexCut$.MODULE$);
		Graph<Object, Double> triangleCount = graph.ops().triangleCount();
		triangleCount.vertices().toJavaRDD().collect().forEach(System.out::println);

	}

	public int getPartition(long src, long dst, int numParts) {
		return 1;
	}

	public static boolean readTextEdgeFile(List<Edge<Double>> edgeList, String txtFileName)
			throws FileNotFoundException, IOException {
		String line = "";
		String txtSplitBy = " ";
		boolean removedBOM = false;
		try (BufferedReader br = new BufferedReader(new FileReader(txtFileName))) {
			while ((line = br.readLine()) != null) {
				String[] record = line.split(txtSplitBy);
				if (record.length == 4) {
					if (!removedBOM && record[0] != "0") {

						record[0] = String.valueOf(0);
						removedBOM = true;

					}
					edgeList.add(new Edge<Double>(Integer.parseInt(record[1]), Integer.parseInt(record[2]),
							Double.parseDouble(record[3])));

				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

		return true;

	}

	public static boolean readTextNodeFile(List<Tuple2<Object, String>> nodeList, String txtFileName)
			throws FileNotFoundException, IOException {
		String line = "";
		String txtSplitBy = " ";
		boolean removedBOM = false;
		long counter = 0L;
		try (BufferedReader br = new BufferedReader(new FileReader(txtFileName))) {
			while ((line = br.readLine()) != null) {
				String[] record = line.split(txtSplitBy);
				if (record.length == 4) {
					if (!removedBOM && record[0] != "0") {

						record[0] = String.valueOf(0);
						removedBOM = true;

					}
					nodeList.add(new Tuple2<Object, String>(counter, record[0]));
					counter++;

				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

		return true;

	}
}