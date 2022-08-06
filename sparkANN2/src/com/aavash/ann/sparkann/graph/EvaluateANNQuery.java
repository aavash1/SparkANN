package com.aavash.ann.sparkann.graph;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;

import com.aavash.ann.sparkann.algorithm.ANNNaive;
import com.aavash.ann.sparkann.algorithm.NearestNeighbor;
import com.ann.sparkann.framework.RoadObject;
import com.ann.sparkann.framework.CoreGraph;

import scala.Tuple2;

public class EvaluateANNQuery implements
		Function2<CoreGraph, List<Tuple2<Object, Map<Object, Map<Object, ArrayList<RoadObject>>>>>, Tuple2<Object, Object>> {

	private CoreGraph cGraph;

	int edgeCounter = 0;
	int objGlobalCounter = 0;

	@Override
	public Tuple2<Object, Object> call(CoreGraph coGraph,
			List<Tuple2<Object, Map<Object, Map<Object, ArrayList<RoadObject>>>>> graphWithObject) throws Exception {
		// TODO Auto-generated method stub
		try {
			this.cGraph = coGraph;
			int edgeCounter = 0;
			int objGlobalcounter = 0;

			NearestNeighbor nn = new NearestNeighbor();

			if (true) {
				for (Integer edgeId : coGraph.getObjectsOnEdges().keySet()) {
					edgeCounter++;
					// System.out.println(
					// "Completed Number of Edges: " + edgeCounter + " out of " +
					// m_graph.getObjectsOnEdges().size());
					int objCounter = 0;
					for (RoadObject trueObj : coGraph.getTrueObjectsOnEdgeSortedByDist(edgeId)) {
						objCounter++;
						objGlobalCounter++;
//						System.out.println(objCounter + " completed Objects on Edge: " + edgeId + " out of "
//								+ m_graph.getTrueObjectsIdOnGivenEdge(edgeId).size() + "; Total completed Objs: "
//								+ objGlobalCounter + " / " + m_graph.getTotalNumberOfTrueObjects());
						int nearestFalseObjId = nn.getNearestFalseObjectIdToGivenObjOnMap(coGraph,
								trueObj.getObjectId());// .getNearestFalseObjectIdToGivenObjOnMap(graph,
														// trueObj.getObjectId());

						return new Tuple2<Object, Object>(trueObj.getObjectId(), nearestFalseObjId);
					}
				}

			}

		} catch (Exception e) {
			System.out.println("Exception error: " + e);
			// TODO: handle exception
		}
		return null;
	}

}
