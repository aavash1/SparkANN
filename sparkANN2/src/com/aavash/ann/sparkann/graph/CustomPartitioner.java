package com.aavash.ann.sparkann.graph;

import org.apache.spark.Partitioner;

public class CustomPartitioner extends Partitioner {
	private final int numParts;

	public CustomPartitioner(Object i) {
		numParts = (int) i;
	}

	@Override
	public int getPartition(Object key) {
		int partIndex = (int) key;
		return partIndex;
	}

	@Override
	public int numPartitions() {
		// TODO Auto-generated method stub
		return numParts;
	}

}
