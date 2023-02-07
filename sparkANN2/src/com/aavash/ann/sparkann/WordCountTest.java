package com.aavash.ann.sparkann;

import java.util.Arrays;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.HashPartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class WordCountTest {

	public static void main(String[] args) {

		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		SparkConf config = new SparkConf().setAppName("word count").setMaster("local[*]");

//		SparkConf config = new SparkConf().setAppName("word count").set("spark.shuffle.service.enabled", "false")
//				.set("spark.dynamicAllocation.enabled", "false").set("spark.submit.deployMode", "cluster")
//				.setMaster("spark://210.107.197.210:7077").set("spark.blockManager.port", "10025").set("spark.driver.port", "10027").set("spark.executor.memory", "512M")
//				.set("spark.cores.max", "4");
//		// .set("spark.locality.wait", "0")
//				.set("spark.submit.deployMode", "cluster").set("spark.driver.maxResultSize", "512M")
//				.set("spark.executor.memory", "512M").setMaster("spark://210.107.197.210:7077")
//				.set("spark.cores.max", "4").set("spark.blockManager.port", "10025")
//				.set("spark.driver.blockManager.port", "10026").set("spark.driver.port", "10027")
//				.set("spark.shuffle.service.enabled", "false").set("spark.dynamicAllocation.enabled", "false");

		JavaSparkContext jsc = new JavaSparkContext(config);
		/// SparkANN/test.txt
		
		
		JavaRDD<String> inputFile = jsc.textFile("/home/aavash/git/SparkANN/sparkANN2/Dataset/test.txt");
		JavaPairRDD<String, Integer> flattenPairs = inputFile.flatMapToPair(text -> Arrays.asList(text.split(" "))
				.stream().map(word -> new Tuple2<String, Integer>(word, 1)).iterator());
		JavaPairRDD<String, Integer> wordCountRDD = flattenPairs.reduceByKey((v1, v2) -> v1 + v2);
		JavaPairRDD<String, Integer> afterPart = wordCountRDD.partitionBy(new HashPartitioner(2));
		afterPart.foreach(x -> System.out.print(x._1() + " " + x._2()));

	}

}
