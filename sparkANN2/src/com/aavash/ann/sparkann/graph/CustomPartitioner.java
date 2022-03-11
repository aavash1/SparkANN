package com.aavash.ann.sparkann.graph;

import org.apache.spark.Partitioner;

public class CustomPartitioner extends Partitioner {
	final int maxPartitions = 2;

	@Override
	public int getPartition(Object key) {
		// TODO Auto-generated method stub
		return ((((String) key).length()) % maxPartitions);
	}

	@Override
	public int numPartitions() {
		// TODO Auto-generated method stub
		return maxPartitions;
	}

}
