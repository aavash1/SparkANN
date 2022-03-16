
package com.aavash.ann.sparkann;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import com.aavash.ann.sparkann.graph.Utilsmanagement;
import scala.Tuple2;
import scala.reflect.ClassTag;

public class METISReader {

	public static <T> void main(String[] args) throws IOException, IOException {

		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("Graph");
		JavaSparkContext javaSparkContext = new JavaSparkContext(conf);
		ClassTag<String> stringTag = scala.reflect.ClassTag$.MODULE$.apply(String.class);
		ClassTag<Integer> intTag = scala.reflect.ClassTag$.MODULE$.apply(Integer.class);
		ClassTag<Double> doubleTag = scala.reflect.ClassTag$.MODULE$.apply(Double.class);

		// SparkANN/METISGraph/MetisTinyGraph
		// String metisInputGraph = "Metisgraph/metis.txt";
		String metisInputGraph = "Metisgraph/Tinygraph.txt";
		String metisPartitionOutputFile = "PartitionDataset/tg_part.txt";
		// Tuple2<Object,List<Integer>> metisHolder1=new
		// Tuple2<Object,List<Integer>>(metisHolder1, null);
		Map<Object, List<Integer>> metisGraph = new HashMap<Object, List<Integer>>();
		metisGraph = Utilsmanagement.readMETISInputGraph(metisInputGraph, metisGraph);
		List<Tuple2<Object, List<Integer>>> mapMetisGraph = new ArrayList(metisGraph.size());

		ArrayList<Integer> partitionIndex = new ArrayList<Integer>();
		partitionIndex = Utilsmanagement.readMETISPartition(metisPartitionOutputFile, partitionIndex);

		// Iterator<Object> itr = metisGraph.keySet().iterator();
		for (Object key : metisGraph.keySet()) {
			List<Integer> neighboringNodes = new ArrayList<Integer>();
			for (int i = 0; i < metisGraph.get(key).size(); i++) {
				neighboringNodes.add(metisGraph.get(key).get(i));
			}
			mapMetisGraph.add(new Tuple2(key, neighboringNodes));
			// System.out.println(key + " " + metisHolder.get(key));

		}
		JavaPairRDD<Object, List<Integer>> mapMetisGraphRDD = javaSparkContext.parallelizePairs(mapMetisGraph);
		// mapMetisGraphRDD.foreach(x -> System.out.println(x._1 + " " + x._2()));

		JavaRDD<Integer> mapMetisPartitionRDD = javaSparkContext.parallelize(partitionIndex);
		// mapMetisPartition.foreach(x -> System.out.println(x));

		Map<Integer, Map<Object, List<Integer>>> metisGraphWithPartIndex = new HashMap<Integer, Map<Object, List<Integer>>>();

		for (Object key : metisGraph.keySet()) {
			System.out.println(key + " " + metisGraph.get(key));
		}
		for (Integer partIndex : partitionIndex) {
			System.out.println(partIndex);
		}

		// Using the partitionOutput as key: this overrwrites the value with same key
		for (Integer index : partitionIndex) {

			for (Object key : metisGraph.keySet()) {
				Map<Object, List<Integer>> nodesAndNeighbors = new HashMap<Object, List<Integer>>();
				if (!metisGraphWithPartIndex.containsKey(index)) {
					nodesAndNeighbors.put(key, metisGraph.get(key));
					metisGraphWithPartIndex.put(index, nodesAndNeighbors);

				} else {
					nodesAndNeighbors.put(key, metisGraph.get(key));
					metisGraphWithPartIndex.put(index, nodesAndNeighbors);

				}

			}

		}

		// Using the metisInputGraph as key. Change of key doesn't affect.
		Map<Map<Object, List<Integer>>, Integer> metisGraphWithPartIndex2 = new HashMap<Map<Object, List<Integer>>, Integer>();

		int counter = 0;
		for (Object key : metisGraph.keySet()) {
			Map<Object, List<Integer>> nodesAndNeighbors = new HashMap<Object, List<Integer>>();
			nodesAndNeighbors.put(key, metisGraph.get(key));
			int indexOfPartition = partitionIndex.get(counter);
			metisGraphWithPartIndex2.put(nodesAndNeighbors, indexOfPartition);
			counter++;

		}
		for (Map<Object, List<Integer>> map1 : metisGraphWithPartIndex2.keySet()) {
			System.out.println(map1 + " " + metisGraphWithPartIndex2.get(map1));
		}

	}

}
