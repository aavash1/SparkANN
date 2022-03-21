package com.aavash.ann.sparkann.graph;

import org.apache.spark.Partitioner;

public class CustomPartitioner extends Partitioner {
	private final int numParts;

	public CustomPartitioner(int i) {
		numParts = i;
	}

	@Override
	public int getPartition(Object key) {
		int partIndex = ((Integer) key);
		return partIndex;
	}

	@Override
	public int numPartitions() {
		// TODO Auto-generated method stub
		return numParts;
	}

}
