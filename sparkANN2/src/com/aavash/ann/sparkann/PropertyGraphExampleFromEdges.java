package com.aavash.ann.sparkann;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.graphx.Edge;
import org.apache.spark.graphx.Graph;
import org.apache.spark.graphx.PartitionStrategy;
import org.apache.spark.storage.StorageLevel;

import scala.reflect.ClassTag;

public class PropertyGraphExampleFromEdges {
	public static void main(String[] args) throws IOException {
		// System.setProperty("hadoop.home.dir", "C:\\softwares\\Winutils");
		SparkConf conf = new SparkConf().setMaster("local").setAppName("graph");
		JavaSparkContext javaSparkContext = new JavaSparkContext(conf);
		// JavaRDD<String> inputEdgesTextFile =
		// javaSparkContext.textFile("Dataset/SFEdge.txt");

		ClassTag<String> stringTag = scala.reflect.ClassTag$.MODULE$.apply(String.class);
		ClassTag<Integer> intTag = scala.reflect.ClassTag$.MODULE$.apply(Integer.class);
		ClassTag<Double> doubleTag = scala.reflect.ClassTag$.MODULE$.apply(Double.class);

		List<Edge<Double>> edges = new ArrayList<>();
		String inputFileName = "Dataset/SFEdge.txt";

		// edges.add(new Edge<Double>(1, 2, 3.5));
		// edges.add(new Edge<Double>(2, 3, 4.8));
		// edges.add(new Edge<Double>(1, 3, 6.5));
		// edges.add(new Edge<Double>(4, 3, 1.8));
		// edges.add(new Edge<Double>(4, 5, 9.6));
		// edges.add(new Edge<Double>(2, 5, 3.3));

		readTextEdgeFile(edges, inputFileName);

		JavaRDD<Edge<Double>> edgeRDD = javaSparkContext.parallelize(edges);

		Graph<String, Double> graph = Graph.fromEdges(edgeRDD.rdd(), "", StorageLevel.MEMORY_ONLY(),
				StorageLevel.MEMORY_ONLY(), stringTag, doubleTag);

		graph.edges().toJavaRDD().collect().forEach(System.out::println);
		graph.vertices().toJavaRDD().collect().forEach(System.out::println);

		// graph.partitionBy(PartitionStrategy.CanonicalRandomVertexCut$.MODULE$);

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
}