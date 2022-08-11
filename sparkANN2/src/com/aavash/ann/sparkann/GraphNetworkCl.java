package com.aavash.ann.sparkann;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.ann.sparkann.framework.CoreGraph;
import com.ann.sparkann.framework.Node;
import com.ann.sparkann.framework.RoadObject;
import com.ann.sparkann.framework.UtilitiesMgmt;
import com.ann.sparkann.framework.UtilsManagement;
import com.ann.sparkann.framework.cEdge;
import com.google.common.collect.LinkedHashMultimap;

import edu.ufl.cise.bsmock.graph.YenGraph;
import scala.Tuple2;

public class GraphNetworkCl {

	public static void main(String[] args) {

		/**
		 * 1 Pass the path for loading the datasets 1.1 Dataset for graph containing
		 * nodes and edges
		 */
		String nodeDatasetFile = "Dataset/PCManualGraphNodes.txt";
		String edgeDataSetFile = "Dataset/PCManualGraphEdges.txt";

		/**
		 * 1.2 Dataset for METIS graph and Partition Output
		 */
		String metisInputGraph = "Metisgraph/ManualGraph.txt";
		String metisPartitionOutputFile = "PartitionDataset/PCmanualGr_part2.txt";

		/**
		 * Load Graph using CoreGraph Framework, YenGraph for calculating shortest paths
		 */
		CoreGraph cGraph = UtilsManagement.readEdgeTxtFileReturnGraph(edgeDataSetFile);
		YenGraph yGraph = new YenGraph(edgeDataSetFile);

		/**
		 * Create Vertices List from the nodeDataset
		 */
		ArrayList<Node> nodesList = UtilsManagement.readTxtNodeFile(nodeDatasetFile);
		cGraph.setNodesWithInfo(nodesList);

		/**
		 * Generate Random Objects on Edge Data Object=100 Query Object=500 Manual Road
		 * Object is also used for testing
		 */
		// RandomObjectGenerator.generateUniformRandomObjectsOnMap(cGraph, 100, 500);
		String PCManualObject = "Dataset/manualobject/ManualObjectsOnRoad.txt";
		UtilsManagement.readRoadObjectTxtFile1(cGraph, PCManualObject);
		// cGraph.printObjectsOnEdges();

		/**
		 * Read the output of METIS as partitionFile
		 */
		ArrayList<Integer> graphPartitionIndex = new ArrayList<Integer>();
		try {
			graphPartitionIndex = UtilitiesMgmt.readMETISPartition(metisPartitionOutputFile, graphPartitionIndex);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		int[] keys = new int[graphPartitionIndex.size()];
		for (int i = 0; i < cGraph.getAdjancencyMap().size(); i++) {

			keys[i] = (int) cGraph.getAdjancencyMap().keySet().toArray()[i];

		}

		List<Tuple2<Object, Map<Object, Map<Object, Double>>>> adjacencyListWithPartitionIndex = new ArrayList<>(
				graphPartitionIndex.size());

		List<Tuple2<Object, Map<Object, Map<Object, ArrayList<RoadObject>>>>> graphWithRoadObject = new ArrayList<>(
				cGraph.getNodesWithInfo().size());

		LinkedHashMultimap<Object, Object> partitionIndexWithVertexId = LinkedHashMultimap.create();
		Map<Object, Object> vertexIdPartitionIndex = new HashMap<Object, Object>();

		for (int i = 0; i < graphPartitionIndex.size(); i++) {
			Map<Object, Map<Object, Double>> mapForAdjacentNodes = new HashMap<Object, Map<Object, Double>>();
			Map<Object, Double> destinationNodes = new HashMap<Object, Double>();

			Map<Object, Map<Object, ArrayList<RoadObject>>> adjacentEdges = new HashMap<Object, Map<Object, ArrayList<RoadObject>>>();
			Map<Object, ArrayList<RoadObject>> adjacentEdgesWithObject = new HashMap<Object, ArrayList<RoadObject>>();

			for (Integer dstIndex : cGraph.getAdjancencyMap().get(keys[i]).keySet()) {
				destinationNodes.put(Long.valueOf(dstIndex), cGraph.getAdjancencyMap().get(keys[i]).get(dstIndex));

				int adjEdgeId = cGraph.getEdgeId(keys[i], dstIndex);
				adjacentEdgesWithObject.put(Long.valueOf(adjEdgeId), cGraph.getObjectsOnEdges().get(adjEdgeId));

			}

			partitionIndexWithVertexId.put(Long.valueOf(graphPartitionIndex.get(i)), keys[i]);

			vertexIdPartitionIndex.put(keys[i], Long.valueOf(graphPartitionIndex.get(i)));

			mapForAdjacentNodes.put(keys[i], destinationNodes);
			adjacencyListWithPartitionIndex.add(new Tuple2<Object, Map<Object, Map<Object, Double>>>(
					Long.valueOf(graphPartitionIndex.get(i)), mapForAdjacentNodes));

			adjacentEdges.put(keys[i], adjacentEdgesWithObject);
			graphWithRoadObject.add(new Tuple2<Object, Map<Object, Map<Object, ArrayList<RoadObject>>>>(
					Long.valueOf(graphPartitionIndex.get(i)), adjacentEdges));

		}

		// System.out.println(graphWithRoadObject);

		/**
		 * Selecting the Boundaries after graph partitions
		 * 
		 */

		Map<Object, Object> BoundaryNodes = new HashMap<>();
		LinkedList<String> BoundaryNodeList = new LinkedList<>();
		ArrayList<cEdge> BoundaryEdge = new ArrayList<>();
		ArrayList<Object> BoundaryEdgeId = new ArrayList<>();

		for (cEdge selectedEdge : cGraph.getEdgesWithInfo()) {
			int SrcId = selectedEdge.getStartNodeId();
			int DestId = selectedEdge.getEndNodeId();

			if (vertexIdPartitionIndex.get(SrcId) == vertexIdPartitionIndex.get(DestId)) {

			} else {

				BoundaryNodes.put(SrcId, vertexIdPartitionIndex.get(SrcId));
				BoundaryNodes.put(DestId, vertexIdPartitionIndex.get(DestId));
				BoundaryEdge.add(selectedEdge);
				BoundaryEdgeId.add(selectedEdge.getEdgeId());

			}

		}
		System.out.println(BoundaryEdge);

		/**
		 * This map holds partitionIndex as keys and ArrayList of Border vertices as
		 * values
		 **/

		Map<Object, ArrayList<Object>> Boundaries = new HashMap<>();
		LinkedHashMap<Integer, ArrayList<String>> strBoundaries = new LinkedHashMap<>();

		for (Object BoundaryVertex : BoundaryNodes.keySet()) {

			BoundaryNodeList.add(String.valueOf(BoundaryVertex));

			if (Boundaries.isEmpty()) {
				ArrayList<Object> vertices = new ArrayList<Object>();
				ArrayList<String> strVertices = new ArrayList<String>();
				vertices.add(BoundaryVertex);
				strVertices.add(String.valueOf(BoundaryVertex));
				Boundaries.put(BoundaryNodes.get(BoundaryVertex), vertices);
				strBoundaries.put(Integer.parseInt(String.valueOf(BoundaryNodes.get(BoundaryVertex))), strVertices);

			} else if (Boundaries.containsKey(BoundaryNodes.get(BoundaryVertex))) {
				Boundaries.get(BoundaryNodes.get(BoundaryVertex)).add(BoundaryVertex);
				strBoundaries.get(Integer.parseInt(String.valueOf(BoundaryNodes.get(BoundaryVertex))))
						.add(String.valueOf(BoundaryVertex));
			} else if (!(Boundaries.isEmpty()) && (!Boundaries.containsKey(BoundaryNodes.get(BoundaryVertex)))) {
				ArrayList<Object> vertices = new ArrayList<Object>();
				ArrayList<String> strVertices = new ArrayList<String>();
				vertices.add(BoundaryVertex);
				strVertices.add(String.valueOf(BoundaryVertex));
				Boundaries.put(BoundaryNodes.get(BoundaryVertex), vertices);
				strBoundaries.put(Integer.parseInt(String.valueOf(BoundaryNodes.get(BoundaryVertex))), strVertices);

			}
		}

		System.out.println(BoundaryNodeList);

		// |Partition_Index|Source_vertex|Destination_vertex|Edge_Length|ArrayList<Road_Object>|
		List<Tuple2<Object, Map<Object, Map<Object, Tuple2<Double, ArrayList<RoadObject>>>>>> adjacentVerticesForSubgraphs = new ArrayList<>(
				graphPartitionIndex.size());

		for (int i = 0; i < graphPartitionIndex.size(); i++) {

			Map<Object, Map<Object, Tuple2<Double, ArrayList<RoadObject>>>> adjacentVertices = new HashMap<>();
			Map<Object, Tuple2<Double, ArrayList<RoadObject>>> adjacentVertexWithObject = new LinkedHashMap<Object, Tuple2<Double, ArrayList<RoadObject>>>();
			for (Integer dstIndex : cGraph.getAdjancencyMap().get(keys[i]).keySet()) {

				int adjEdgeId = cGraph.getEdgeId(keys[i], dstIndex);

				if (!BoundaryEdgeId.contains(cGraph.getEdgeId(keys[i], dstIndex))) {
					adjacentVertexWithObject.put(Long.valueOf(dstIndex), new Tuple2<Double, ArrayList<RoadObject>>(
							cGraph.getEdgeDistance(keys[i], dstIndex), cGraph.getObjectsOnEdges().get(adjEdgeId)));
				}

			}

			adjacentVertices.put(keys[i], adjacentVertexWithObject);
			adjacentVerticesForSubgraphs
					.add(new Tuple2<Object, Map<Object, Map<Object, Tuple2<Double, ArrayList<RoadObject>>>>>(
							Long.valueOf(graphPartitionIndex.get(i)), adjacentVertices));

		}

		for (Object o : adjacentVerticesForSubgraphs) {
			System.out.println(o);
		}

		List<Tuple2<Object, ArrayList<RoadObject>>> roadObjectList = new ArrayList<>(cGraph.getObjectsOnEdges().size());

		for (Integer edgeId : cGraph.getObjectsOnEdges().keySet()) {
			roadObjectList.add(

					new Tuple2<Object, ArrayList<RoadObject>>((Integer) edgeId,
							cGraph.getObjectsOnEdges().get(edgeId)));
		}

		// cGraph.printEdgesInfo();
		// System.out.println(roadObjectList);

		// System.out.println(BoundaryNodes);
		// System.out.println(BoundaryEdge);
		// System.out.println(strBoundaries);

	}

}