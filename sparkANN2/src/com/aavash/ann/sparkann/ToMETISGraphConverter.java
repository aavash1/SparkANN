package com.aavash.ann.sparkann;

import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.tinspin.*;
import org.tinspin.index.rtree.RTree;

import com.ann.sparkann.framework.cEdge;

import avro.shaded.com.google.common.base.Stopwatch;
import edu.ufl.cise.bsmock.graph.YenGraph;
import edu.ufl.cise.bsmock.graph.ksp.Yen;
import edu.ufl.cise.bsmock.graph.util.Path;

import com.ann.sparkann.framework.CoreGraph;
import com.ann.sparkann.framework.Node;
import com.ann.sparkann.framework.UtilsManagement;

public class ToMETISGraphConverter {

	public static void main(String[] args) {
		System.setProperty("java.util.Arrays.useLegacyMergeSort", "true");
		String nodeDatasetFile = "Dataset/SFNodes.txt";
		String edgeDataSetFile = "Dataset/SFEdge.txt";

		CoreGraph cGraph = UtilsManagement.readEdgeTxtFileReturnGraph(edgeDataSetFile);
		cGraph.setDatasetName("SanFrancisco");
		ArrayList<Node> nodesList = UtilsManagement.readTxtNodeFile(nodeDatasetFile);
		cGraph.setNodesWithInfo(nodesList);

		String graphName = cGraph.getDatasetName();
		// int nodeSize = cGraph.getNodesWithInfo().size();
		// int edgeSize = cGraph.getEdgesWithInfo().size();

		// String graphOutput = "toMETISGraph/" + graphName + "_METIS_1" + ".graph";

		String convertedNodeGraph = "convertedGraphs/" + graphName + "Node" + ".txt";
		String convertedEdgeGraph = "convertedGraphs/" + graphName + "Edge" + ".txt";

		UtilsManagement.convertNodeToTXTFile(cGraph, cGraph.getNodesWithInfo(), convertedNodeGraph);
		UtilsManagement.convertEdgeToTXTFile(cGraph, cGraph.getEdgesWithInfo(), convertedEdgeGraph);

		String convertedNodeFile = "convertedGraphs/" + graphName + "Node" + ".txt";
		String convertedEdgeFile = "convertedGraphs/" + graphName + "Edge" + ".txt";

		CoreGraph convertedGraph = UtilsManagement.readEdgeTxtFileReturnGraph(convertedEdgeFile);
		convertedGraph.setDatasetName(graphName);
		ArrayList<Node> convertedNodes = UtilsManagement.readTxtNodeFile(convertedNodeFile);
		convertedGraph.setNodesWithInfo(convertedNodes);

		int nodeSize = cGraph.getNodesWithInfo().size();
		int edgeSize = cGraph.getEdgesWithInfo().size();
		
		String graphOutput = "toMETISGraph/" + graphName + "conv_METIS_1" + ".graph";

		ArrayList<int[]> rows1 = UtilsManagement.createAdjArrayForMETISConversion(convertedGraph);
		UtilsManagement.convertDatasetToMETISFormat(graphOutput, rows1, nodeSize, edgeSize);

	}

}
