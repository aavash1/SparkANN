
package com.aavash.ann.sparkann;

import java.io.IOException;
import java.util.ArrayList;

import java.util.HashMap;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.google.common.collect.LinkedHashMultimap;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.aavash.ann.sparkann.graph.Utilsmanagement;

import scala.Tuple2;

public class METISReader {

	@SuppressWarnings("unchecked")
	public static <T> void main(String[] args) throws IOException, IOException {

		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("Graph");
		try (JavaSparkContext javaSparkContext = new JavaSparkContext(conf)) {
			// SparkANN/METISGraph/MetisTinyGraph
			// String metisInputGraph = "Metisgraph/metis.txt";
			// String metisPartitionOutputFile = "PartitionDataset/metis.txt";
			String metisInputGraph = "Metisgraph/Tinygraph.txt";
			String metisPartitionOutputFile = "PartitionDataset/tg_part.txt";

			// 1. Read METIS graph input
			Map<Object, List<Integer>> metisGraph = new HashMap<Object, List<Integer>>();
			metisGraph = Utilsmanagement.readMETISInputGraph(metisInputGraph, metisGraph);
			// 1.2 Creating RDD of the metisGraph
			List<Tuple2<Object, List<Integer>>> mapMetisGraph = new ArrayList(metisGraph.size());
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

			// 2. Read the output of METIS as partitionFile
			ArrayList<Integer> partitionIndex = new ArrayList<Integer>();
			partitionIndex = Utilsmanagement.readMETISPartition(metisPartitionOutputFile, partitionIndex);
			// 2.1 Create the RDD of the List.
			JavaRDD<Integer> mapMetisPartitionRDD = javaSparkContext.parallelize(partitionIndex);
			// mapMetisPartition.foreach(x -> System.out.println(x));

			// 3. Storing the partitionIndex and metisGraphInput in the same
			// LinkedHashMultiMap.
			LinkedHashMultimap<Object, Map<Object, List<Integer>>> metisGraphWithPartitionIndex = LinkedHashMultimap
					.create();
			for (int i = 0; i < partitionIndex.size(); i++) {
				Map<Object, List<Integer>> nodesAndNeighbors = new HashMap<Object, List<Integer>>();
				nodesAndNeighbors.put(metisGraph.keySet().toArray()[i],
						(List<Integer>) metisGraph.values().toArray()[i]);
				metisGraphWithPartitionIndex.put(partitionIndex.get(i), nodesAndNeighbors);
			}
			
			System.out.println(metisGraphWithPartitionIndex);

			// 4. Creating an RDD of the metisGraphWithPartitionIndex.
			List<Tuple2<Object, Map<Integer, List<Integer>>>> mapMetisGraphWithPartitionIndexR = new ArrayList<>(
					metisGraphWithPartitionIndex.size());

		}

	}

}
