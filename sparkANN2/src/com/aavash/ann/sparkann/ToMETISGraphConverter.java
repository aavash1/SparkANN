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

	public static void main(String[] args) throws IOException {
		System.setProperty("java.util.Arrays.useLegacyMergeSort", "true");

		String nodeDatasetFile = "Dataset/SJNodes.txt";
		String edgeDataSetFile = "Dataset/SJEdge.txt";

		CoreGraph coreGraph = UtilsManagement.readEdgeTxtFileReturnGraph(edgeDataSetFile);

		ArrayList<Node> convertedNodes = UtilsManagement.readTxtNodeFile(nodeDatasetFile);
		coreGraph.setNodesWithInfo(convertedNodes);
		coreGraph.setDatasetName("SanJoaquin");

		int nodeSize1 = coreGraph.getNumberOfNodes() - 1;
		int edgeSize1 = coreGraph.getNumberOfEdges();

		String convertedNodeFile = "convertedGraphs/" + coreGraph.getDatasetName() + "_Nodes_changed_" + ".txt";
		String convertedEdgeFile = "convertedGraphs/" + coreGraph.getDatasetName() + "_Edges_changed_" + ".txt";

		UtilsManagement.convertEdgeToTXTFile(coreGraph, coreGraph.getEdgesWithInfo(), convertedEdgeFile);
		UtilsManagement.convertNodeToTXTFile(coreGraph, convertedNodes, convertedNodeFile);

		CoreGraph convertedGraph = UtilsManagement.readEdgeTxtFileReturnGraph(convertedEdgeFile);

		ArrayList<Node> NodesAfterConversion = UtilsManagement.readTxtNodeFile(convertedNodeFile);
		convertedGraph.setNodesWithInfo(convertedNodes);
		convertedGraph.setDatasetName("SanJoaquin");

		// convertedGraph.printEdgesInfo();

//		String changedNodeFile = "convertedGraphs/" + "NANode_changed" + ".txt";
//		String changedEdgeFile = "convertedGraphs/" + "NAEdge_changed" + ".txt";
//
//		UtilsManagement.writeNodeDataset(convertedGraph, changedNodeFile);
//		UtilsManagement.writeEdgeDataset(convertedGraph, changedEdgeFile);
		int nodeSize = convertedGraph.getNumberOfNodes() - 1;
		int edgeSize = convertedGraph.getNumberOfEdges();

		String graphOutput = "toMETISGraph/" + convertedGraph.getDatasetName() + "_METIS_Format" + ".graph";

		ArrayList<int[]> rows1 = UtilsManagement.createAdjArrayForMETISConversion(convertedGraph);

		UtilsManagement.convertDatasetToMETISFormat(graphOutput, rows1, nodeSize, edgeSize);

	}

}
