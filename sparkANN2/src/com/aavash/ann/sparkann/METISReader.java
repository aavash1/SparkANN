package com.aavash.ann.sparkann;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.aavash.ann.sparkann.graph.Utilsmanagement;

public class METISReader {

	public static void main(String[] args) {
		// SparkANN/METISGraph/MetisTinyGraph

		String metisInputGraph = "Metisgraph/Tinygraph.txt";
		HashMap<Object, ArrayList<Integer>> metisHolder = new HashMap<Object, ArrayList<Integer>>();

		metisHolder = Utilsmanagement.readMETISInputGraph(metisInputGraph, metisHolder);

//		Iterator<Object> itr = metisHolder.keySet().iterator();
		for (Object key : metisHolder.keySet()) {
			System.out.println(key + " " + metisHolder.get(key));

		}

	}

}
