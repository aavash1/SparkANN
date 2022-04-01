
package com.aavash.ann.sparkann;

import java.io.IOException;
import java.util.ArrayList;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.google.common.collect.LinkedHashMultimap;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.aavash.ann.sparkann.graph.CustomPartitioner;
import com.aavash.ann.sparkann.graph.Utilsmanagement;

import scala.Tuple2;

public class METISReader {

	@SuppressWarnings("unchecked")
	public static <T> void main(String[] args) throws IOException, IOException {

		// SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("Graph");
		SparkConf conf = new SparkConf().setMaster("local").setAppName("Graph")
				.set("spark.shuffle.service.enabled", "false").set("spark.driver.blockManager.port", "10026")
				.set("spark.driver.port", "10027").set("spark.cores.max", "3").set("spark.executor.memory", "1G")
				.set("spark.driver.host", "210.107.197.209").set("spark.shuffle.service.enabled", "false")
				.set("spark.dynamicAllocation.enabled", "false").set("spark.shuffle.blockTransferService", "nio");
		try (JavaSparkContext javaSparkContext = new JavaSparkContext(conf)) {
			// SparkANN/METISGraph/MetisTinyGraph
			// String metisInputGraph = "Metisgraph/metis.txt";
			// String metisPartitionOutputFile = "PartitionDataset/metis.txt";
			String metisInputGraph = "Metisgraph/Tinygraph.txt";
			String metisPartitionOutputFile = "PartitionDataset/tg_part.txt";

			// 1. Read METIS graph input
			Map<Integer, List<Integer>> metisGraph = new HashMap<Integer, List<Integer>>();
			metisGraph = Utilsmanagement.readMETISInputGraph(metisInputGraph, metisGraph);
			// 1.2 Creating RDD of the metisGraph
			List<Tuple2<Integer, List<Integer>>> mapMetisGraph = new ArrayList(metisGraph.size());
			for (Object key : metisGraph.keySet()) {
				List<Integer> neighboringNodes = new ArrayList<Integer>();
				for (int i = 0; i < metisGraph.get(key).size(); i++) {
					neighboringNodes.add(metisGraph.get(key).get(i));
				}
				mapMetisGraph.add(new Tuple2(key, neighboringNodes));
				// System.out.println(key + " " + metisHolder.get(key));

			}
			JavaPairRDD<Integer, List<Integer>> mapMetisGraphRDD = javaSparkContext.parallelizePairs(mapMetisGraph);
			// mapMetisGraphRDD.foreach(x -> System.out.println(x._1 + " " + x._2()));

			// 2. Read the output of METIS as partitionFile
			ArrayList<Integer> partitionIndex = new ArrayList<Integer>();
			partitionIndex = Utilsmanagement.readMETISPartition(metisPartitionOutputFile, partitionIndex);
			// 2.1 Create the RDD of the List.
			JavaRDD<Integer> mapMetisPartitionRDD = javaSparkContext.parallelize(partitionIndex);
			// mapMetisPartition.foreach(x -> System.out.println(x));

			// 3. Storing the partitionIndex and metisGraphInput in the same
			// LinkedHashMultiMap.
			LinkedHashMultimap<Integer, Map<Integer, List<Integer>>> metisGraphWithPartitionIndex = LinkedHashMultimap
					.create();
			for (int i = 0; i < partitionIndex.size(); i++) {
				Map<Integer, List<Integer>> nodesAndNeighbors = new HashMap<Integer, List<Integer>>();
				nodesAndNeighbors.put((Integer) metisGraph.keySet().toArray()[i],
						(List<Integer>) metisGraph.values().toArray()[i]);
				metisGraphWithPartitionIndex.put(partitionIndex.get(i), nodesAndNeighbors);
			}

			// System.out.println("The size of metisGraphWithPartition is: " +
			// metisGraphWithPartitionIndex.size());

			// 4. Creating an RDD of the metisGraphWithPartitionIndex.
			List<Tuple2<Integer, Map<Integer, List<Integer>>>> mapMetisGraphWithPartitionIndex = new ArrayList<>(
					metisGraphWithPartitionIndex.size());

			// 4.1 Converting the HashMap to scala.Tuple2 and adding object to the list.
			for (Map.Entry<Integer, Map<Integer, List<Integer>>> i : metisGraphWithPartitionIndex.entries()) {
				mapMetisGraphWithPartitionIndex
						.add(new Tuple2<Integer, Map<Integer, List<Integer>>>(i.getKey(), i.getValue()));
			}

			// 4.2 Creating a JavaPairRDD.
			JavaPairRDD<Integer, Map<Integer, List<Integer>>> metisGraphWithPartitionIndexRDD = javaSparkContext
					.parallelizePairs(mapMetisGraphWithPartitionIndex);

			metisGraphWithPartitionIndexRDD.foreach(x -> System.out.println(x._1() + " " + x._2()));

			JavaPairRDD<Integer, Map<Integer, List<Integer>>> customPartitioned = metisGraphWithPartitionIndexRDD
					.partitionBy(new CustomPartitioner(2));

			JavaRDD<Integer> result = customPartitioned.mapPartitionsWithIndex((idx, i) -> {
				List<Integer> partitionCheckList = new ArrayList<>();
				while (i.hasNext()) {
					partitionCheckList.add(i.next()._1);
				}
				return partitionCheckList.iterator();
			}, true);

			System.out.println(result.collect());

			System.out.println("Num partitions " + result.getNumPartitions());

		}

	}

}
