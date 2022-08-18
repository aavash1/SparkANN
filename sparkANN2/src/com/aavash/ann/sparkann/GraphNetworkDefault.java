package com.aavash.ann.sparkann;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;

import com.aavash.ann.sparkann.graph.CustomPartitioner;
import com.ann.sparkann.framework.CoreGraph;
import com.ann.sparkann.framework.Node;
import com.ann.sparkann.framework.RoadObject;
import com.ann.sparkann.framework.UtilitiesMgmt;
import com.ann.sparkann.framework.UtilsManagement;
import com.ann.sparkann.framework.cEdge;

import breeze.optimize.linear.LinearProgram.Result;
import edu.ufl.cise.bsmock.graph.YenGraph;
import edu.ufl.cise.bsmock.graph.ksp.Yen;
import edu.ufl.cise.bsmock.graph.util.Path;
import scala.Tuple2;
import scala.Tuple4;
import scala.Tuple5;
import scala.reflect.ClassTag;

public class GraphNetworkDefault {

	public static void main(String[] args) throws Exception {

		// Defining tags

		ClassTag<Double> doubleTag = scala.reflect.ClassTag$.MODULE$.apply(Double.class);
		ClassTag<Node> nodeTag = scala.reflect.ClassTag$.MODULE$.apply(Node.class);

		/**
		 * 1 Pass the path for loading the datasets 1.1 Dataset for graph containing
		 * nodes and edges
		 */
		String nodeDatasetFile = "Dataset/TinygraphNodes.txt";
		String edgeDataSetFile = "Dataset/TinygraphEdges.txt";

		/**
		 * 1.2 Dataset for METIS graph and Partition Output
		 */
		String metisInputGraph = "Metisgraph/ManualGraph.txt";
		String metisPartitionOutputFile = "PartitionDataset/tg_part.txt";

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
		// cGraph.printEdgesInfo();

		/**
		 * Test the YenGraph for finding SPF between two vertex
		 */

		/**
		 * Generate Random Objects on Edge Data Object=100 Query Object=500 Manual Road
		 * Object is also used for testing
		 */
		// RandomObjectGenerator.generateUniformRandomObjectsOnMap(cGraph, 100, 500);

		String PCManualObject = "Dataset/manualobject/ManualObjectOnTinyGraph.txt";
		UtilsManagement.readRoadObjectTxtFile1(cGraph, PCManualObject);
		// cGraph.printObjectsOnEdges();

		/**
		 * Read the output of METIS as partitionFile
		 */
		ArrayList<Integer> graphPartitionIndex = new ArrayList<Integer>();
		graphPartitionIndex = UtilitiesMgmt.readMETISPartition(metisPartitionOutputFile, graphPartitionIndex);

		int[] partitionIndexKey = new int[graphPartitionIndex.size()];
		for (int i = 0; i < cGraph.getAdjancencyMap().size(); i++) {

			partitionIndexKey[i] = (int) cGraph.getAdjancencyMap().keySet().toArray()[i];

		}

		Map<Object, Object> vertexIdPartitionIndex = new HashMap<Object, Object>();
		for (int i = 0; i < graphPartitionIndex.size(); i++) {
			vertexIdPartitionIndex.put(partitionIndexKey[i], Long.valueOf(graphPartitionIndex.get(i)));
		}

		// Depending upon the size of cluster, CustomPartitionSize can be changed
		int CustomPartitionSize = 2;

		/**
		 * Selecting the Boundaries after graph partitions
		 * 
		 */

		Map<Object, Object> boundaryPairVertices = new HashMap<>();
		LinkedList<String> boundaryVerticesList = new LinkedList<>();
		ArrayList<cEdge> BoundaryEdge = new ArrayList<>();
		ArrayList<Object> BoundaryEdgeId = new ArrayList<>();

		for (cEdge selectedEdge : cGraph.getEdgesWithInfo()) {
			int SrcId = selectedEdge.getStartNodeId();
			int DestId = selectedEdge.getEndNodeId();

			if (vertexIdPartitionIndex.get(SrcId) == vertexIdPartitionIndex.get(DestId)) {

			} else {

				boundaryPairVertices.put(SrcId, vertexIdPartitionIndex.get(SrcId));
				boundaryPairVertices.put(DestId, vertexIdPartitionIndex.get(DestId));
				BoundaryEdge.add(selectedEdge);
				BoundaryEdgeId.add(selectedEdge.getEdgeId());

			}

		}

		/**
		 * This map holds partitionIndex as keys and ArrayList of Border vertices as
		 * values
		 **/

		Map<Object, ArrayList<Object>> boundaryVertexPairs = new HashMap<>();
		LinkedHashMap<Integer, ArrayList<String>> stringBoundaryVertices = new LinkedHashMap<>();

		for (Object BoundaryVertex : boundaryPairVertices.keySet()) {

			boundaryVerticesList.add(String.valueOf(BoundaryVertex));

			if (boundaryVertexPairs.isEmpty()) {
				ArrayList<Object> vertices = new ArrayList<Object>();
				ArrayList<String> strVertices = new ArrayList<String>();
				vertices.add(BoundaryVertex);
				strVertices.add(String.valueOf(BoundaryVertex));
				boundaryVertexPairs.put(boundaryPairVertices.get(BoundaryVertex), vertices);
				stringBoundaryVertices.put(Integer.parseInt(String.valueOf(boundaryPairVertices.get(BoundaryVertex))),
						strVertices);

			} else if (boundaryVertexPairs.containsKey(boundaryPairVertices.get(BoundaryVertex))) {
				boundaryVertexPairs.get(boundaryPairVertices.get(BoundaryVertex)).add(BoundaryVertex);
				stringBoundaryVertices.get(Integer.parseInt(String.valueOf(boundaryPairVertices.get(BoundaryVertex))))
						.add(String.valueOf(BoundaryVertex));
			} else if (!(boundaryVertexPairs.isEmpty())
					&& (!boundaryVertexPairs.containsKey(boundaryPairVertices.get(BoundaryVertex)))) {
				ArrayList<Object> vertices = new ArrayList<Object>();
				ArrayList<String> strVertices = new ArrayList<String>();
				vertices.add(BoundaryVertex);
				strVertices.add(String.valueOf(BoundaryVertex));
				boundaryVertexPairs.put(boundaryPairVertices.get(BoundaryVertex), vertices);
				stringBoundaryVertices.put(Integer.parseInt(String.valueOf(boundaryPairVertices.get(BoundaryVertex))),
						strVertices);

			}
		}

//		System.out.println(boundaryPairVertices);
//		System.out.println(BoundaryEdge);
//		System.out.println(stringBoundaryVertices);

		/**
		 * |Partition_Index|Source_vertex|Destination_vertex|Edge_Length|ArrayList<Road_Object>|
		 */
		List<Tuple2<Object, Map<Object, Map<Object, Tuple2<Double, ArrayList<RoadObject>>>>>> adjacentVerticesForSubgraphs = new ArrayList<>(
				graphPartitionIndex.size());

		List<Tuple5<Object, Object, Object, Double, ArrayList<RoadObject>>> tuplesForSubgraphs = new ArrayList<>(
				graphPartitionIndex.size());

		List<Tuple2<Object, Tuple4<Object, Object, Double, ArrayList<RoadObject>>>> newTupleForSubgraphs = new ArrayList<>(
				graphPartitionIndex.size());

		for (int i = 0; i < graphPartitionIndex.size(); i++) {

			for (Integer dstIndex : cGraph.getAdjancencyMap().get(partitionIndexKey[i]).keySet()) {
				int adjEdgeId = cGraph.getEdgeId(partitionIndexKey[i], dstIndex);
				if (!BoundaryEdgeId.contains(cGraph.getEdgeId(partitionIndexKey[i], dstIndex))) {
					tuplesForSubgraphs.add(new Tuple5<Object, Object, Object, Double, ArrayList<RoadObject>>(
							Long.valueOf(graphPartitionIndex.get(i)), partitionIndexKey[i], dstIndex,
							cGraph.getEdgeDistance(partitionIndexKey[i], dstIndex),
							cGraph.getObjectsOnEdges().get(adjEdgeId)));

					Tuple4<Object, Object, Double, ArrayList<RoadObject>> nt = new Tuple4<Object, Object, Double, ArrayList<RoadObject>>(
							partitionIndexKey[i], dstIndex, cGraph.getEdgeDistance(partitionIndexKey[i], dstIndex),
							cGraph.getObjectsOnEdges().get(adjEdgeId));

					newTupleForSubgraphs.add(new Tuple2<Object, Tuple4<Object, Object, Double, ArrayList<RoadObject>>>(
							Long.valueOf(graphPartitionIndex.get(i)), nt));

				}

			}

		}

//		for (int i = 0; i < newTupleForSubgraphs.size(); i++) {
//			System.out.println(tuplesForSubgraphs.get(i));
//		}

		for (int i = 0; i < graphPartitionIndex.size(); i++) {

			Map<Object, Map<Object, Tuple2<Double, ArrayList<RoadObject>>>> adjacentVertices = new HashMap<>();
			Map<Object, Tuple2<Double, ArrayList<RoadObject>>> adjacentVertexWithObject = new LinkedHashMap<Object, Tuple2<Double, ArrayList<RoadObject>>>();
			for (Integer dstIndex : cGraph.getAdjancencyMap().get(partitionIndexKey[i]).keySet()) {

				int adjEdgeId = cGraph.getEdgeId(partitionIndexKey[i], dstIndex);

				if (!BoundaryEdgeId.contains(cGraph.getEdgeId(partitionIndexKey[i], dstIndex))) {
					adjacentVertexWithObject.put(Long.valueOf(dstIndex),
							new Tuple2<Double, ArrayList<RoadObject>>(
									cGraph.getEdgeDistance(partitionIndexKey[i], dstIndex),
									cGraph.getObjectsOnEdges().get(adjEdgeId)));
				}

			}

			adjacentVertices.put(partitionIndexKey[i], adjacentVertexWithObject);
			adjacentVerticesForSubgraphs
					.add(new Tuple2<Object, Map<Object, Map<Object, Tuple2<Double, ArrayList<RoadObject>>>>>(
							Long.valueOf(graphPartitionIndex.get(i)), adjacentVertices));

		}

		/**
		 * Using the Boundaries hashmap <PartitionIndex, ArrayList<BorderVertices> to
		 * find the shortest path from one partition to another partition Storing all
		 * the vertex that are in shortest path list in a separate ArrayList
		 */
		ArrayList<List<Path>> shortestPathList = runSPF(yGraph, stringBoundaryVertices, CustomPartitionSize);
		ArrayList<List<Path>> shortestPathList1 = runSP(yGraph, boundaryVerticesList, CustomPartitionSize);
		// System.out.println("Verify the shortest-paths");
//		for (List<Path> p1 : shortestPathList) {
//			System.out.println(p1 + " ");
//		}

		List<Integer> shortestpathsUnion = unifyAllShortestPaths(shortestPathList);
		System.out.println();

		/**
		 * Load Spark Necessary Items
		 */
		Logger.getLogger("org.apache").setLevel(Level.WARN);

		SparkConf config = new SparkConf().setMaster("local[*]").setAppName("Final Graph")
				.set("spark.history.fs.logDirectory", "file:///D:/spark-logs")
				.set("spark.eventLog.dir", "file:///D:/spark-logs").set("spark.eventLog.enabled", "true");

		try (JavaSparkContext jscontext = new JavaSparkContext(config)) {

			JavaRDD<String> BoundaryVertexRDD = jscontext.parallelize(boundaryVerticesList);
			JavaRDD<cEdge> BoundaryEdgeRDD = jscontext.parallelize(BoundaryEdge);

//			BoundaryVertexRDD.collect().forEach(x -> System.out.print(x + " "));
//			System.out.println(" ");
//			BoundaryEdgeRDD.collect().forEach(x -> System.out.print(x.getEdgeId() + " "));
//			System.out.println(" ");

			/**
			 * Creating Embedded Network Graph: 1) Initially create a Virtual Vertex 2)
			 * Create a Embedded graph connecting VIRTUAL NODE to every other boundary Nodes
			 * 3) Set the weights as ZERO 4) Run the traversal from VIRTUAL NODE to other
			 * BOUNDARY NODES 5) Calculate the distance to the nearest node and store it in
			 * a array Tuple2<Object,Map<Object,Double>> VirtualGraph
			 **/

			/**
			 * Once the graph is created: 1) Combine the GraphRDD with RoadObjectPairRDD,
			 * and BoundaryEdgeRDD. 2) Apply CustomPartitioner function on the newly created
			 * RDD 3)
			 */

			// <Partition_index, edge_id, edge_distance, List<RoadObject>>

			// <Partition_Index_Key, Map<Source_vertex, Map<Destination Vertex,
			// Tuple2<Edge_Length, ArrayList of Random Objects>>
			JavaPairRDD<Object, Iterable<Map<Object, Map<Object, Tuple2<Double, ArrayList<RoadObject>>>>>> adjVertForSubgraphsRDD = jscontext
					.parallelizePairs(adjacentVerticesForSubgraphs).groupByKey()
					.partitionBy(new CustomPartitioner(CustomPartitionSize));

			// adjVertForSubgraphsRDD.foreach(x -> System.out.println(x + "\n"));

			// adjacentVerticesForSubgraphs.forEach(x -> System.out.println(x));

			// applying foreachPartition action on JavaPairRDD

			// adjVertForSubgraphsRDD.collectPartitions(partitionIndexKey)

			JavaPairRDD<Object, Iterable<Tuple2<Object, Tuple4<Object, Object, Double, ArrayList<RoadObject>>>>> parRDD = jscontext
					.parallelize(newTupleForSubgraphs).groupBy(x -> x._1())
					.partitionBy(new CustomPartitioner(CustomPartitionSize));

			JavaRDD<Tuple5<Object, Object, Object, Double, ArrayList<RoadObject>>> newRDD = jscontext
					.parallelize(tuplesForSubgraphs);

			JavaPairRDD<Object, Tuple4<Object, Object, Double, ArrayList<RoadObject>>> pairRDD = newRDD
					.mapPartitionsToPair(
							new PairFlatMapFunction<Iterator<Tuple5<Object, Object, Object, Double, ArrayList<RoadObject>>>, Object, Tuple4<Object, Object, Double, ArrayList<RoadObject>>>() {

								/**
								 * 
								 */
								private static final long serialVersionUID = 1L;

								@Override
								public Iterator<Tuple2<Object, Tuple4<Object, Object, Double, ArrayList<RoadObject>>>> call(
										Iterator<Tuple5<Object, Object, Object, Double, ArrayList<RoadObject>>> rowTups)
										throws Exception {

									List<Tuple2<Object, Tuple4<Object, Object, Double, ArrayList<RoadObject>>>> infoListWithKey = new ArrayList<>();
									while (rowTups.hasNext()) {
										Tuple5<Object, Object, Object, Double, ArrayList<RoadObject>> element = rowTups
												.next();
										Object partitionIndex = element._1();
										Object sourceVertex = element._2();
										Object destVertex = element._3();
										Double edgeLength = element._4();
										ArrayList<RoadObject> listOfRoadObjects = element._5();

										Tuple4<Object, Object, Double, ArrayList<RoadObject>> t = new Tuple4<Object, Object, Double, ArrayList<RoadObject>>(
												sourceVertex, destVertex, edgeLength, listOfRoadObjects);
										infoListWithKey.add(
												new Tuple2<Object, Tuple4<Object, Object, Double, ArrayList<RoadObject>>>(
														partitionIndex, t));
									}
									// TODO Auto-generated method stub
									return infoListWithKey.iterator();
								}

							});

			JavaPairRDD<Object, Iterable<Tuple4<Object, Object, Double, ArrayList<RoadObject>>>> toCreateSubgraphRDD = pairRDD
					.groupByKey().partitionBy(new CustomPartitioner(CustomPartitionSize));

			// toCreateSubgraphRDD.foreach(x -> System.out.println(x));

			toCreateSubgraphRDD.foreachPartition(
					new VoidFunction<Iterator<Tuple2<Object, Iterable<Tuple4<Object, Object, Double, ArrayList<RoadObject>>>>>>() {

						/**
						 * 
						 */
						private static final long serialVersionUID = 1L;
						CoreGraph subgraph0 = new CoreGraph();
						CoreGraph subGraph1 = new CoreGraph();

						@Override
						public void call(
								Iterator<Tuple2<Object, Iterable<Tuple4<Object, Object, Double, ArrayList<RoadObject>>>>> eachTuple)
								throws Exception {

							int sourceVertex;
							int destVertex;
							double edgeLength;

							int roadObjectId;
							boolean roadObjectType;
							double distanceFromStart;
							// TODO Auto-generated method stub

							ArrayList<Integer> addedObjects = new ArrayList<Integer>();

							while (eachTuple.hasNext()) {
								Tuple2<Object, Iterable<Tuple4<Object, Object, Double, ArrayList<RoadObject>>>> theTup = eachTuple
										.next();
								if ((Integer.parseInt(String.valueOf(theTup._1()))) == 1) {

									Iterator<Tuple4<Object, Object, Double, ArrayList<RoadObject>>> iter = theTup._2()
											.iterator();
									while (iter.hasNext()) {
										Tuple4<Object, Object, Double, ArrayList<RoadObject>> listInfo = iter.next();
										sourceVertex = Integer.parseInt(String.valueOf(listInfo._1()));
										destVertex = Integer.parseInt(String.valueOf(listInfo._2()));
										edgeLength = listInfo._3();
										ArrayList<RoadObject> roadObjList = listInfo._4();

										subgraph0.addEdge(sourceVertex, destVertex, edgeLength);
										int currentEdgeId = subgraph0.getEdgeId(sourceVertex, destVertex);

										if (!roadObjList.isEmpty()) {
											for (int i = 0; i < roadObjList.size(); i++) {
												roadObjectId = roadObjList.get(i).getObjectId();
												roadObjectType = roadObjList.get(i).getType();
												distanceFromStart = roadObjList.get(i).getDistanceFromStartNode();

												RoadObject rn0 = new RoadObject();
												rn0.setObjId(roadObjectId);
												rn0.setType(roadObjectType);
												rn0.setDistanceFromStartNode(distanceFromStart);

												if (addedObjects.isEmpty()) {
													subgraph0.addObjectOnEdge(currentEdgeId, rn0);
												} else if ((!addedObjects.isEmpty())
														&& (!addedObjects.contains(rn0.getObjectId()))) {
													subgraph0.addObjectOnEdge(currentEdgeId, rn0);
												}

											}
										}

									}

									// System.out.println(theTup._1());
								} else if ((Integer.parseInt(String.valueOf(theTup._1()))) == 0) {
									Iterator<Tuple4<Object, Object, Double, ArrayList<RoadObject>>> iter = theTup._2()
											.iterator();
									while (iter.hasNext()) {
										Tuple4<Object, Object, Double, ArrayList<RoadObject>> listInfo = iter.next();
										sourceVertex = Integer.parseInt(String.valueOf(listInfo._1()));
										destVertex = Integer.parseInt(String.valueOf(listInfo._2()));
										edgeLength = listInfo._3();
										ArrayList<RoadObject> roadObjList = listInfo._4();

										subGraph1.addEdge(sourceVertex, destVertex, edgeLength);
										int currentEdgeId = subGraph1.getEdgeId(sourceVertex, destVertex);

										if (!roadObjList.isEmpty()) {
											for (int i = 0; i < roadObjList.size(); i++) {
												roadObjectId = roadObjList.get(i).getObjectId();
												roadObjectType = roadObjList.get(i).getType();
												distanceFromStart = roadObjList.get(i).getDistanceFromStartNode();

												RoadObject rn1 = new RoadObject();
												rn1.setObjId(roadObjectId);
												rn1.setType(roadObjectType);
												rn1.setDistanceFromStartNode(distanceFromStart);

												if (addedObjects.isEmpty()) {
													subGraph1.addObjectOnEdge(currentEdgeId, rn1);
												} else if ((!addedObjects.isEmpty())
														&& (!addedObjects.contains(rn1.getObjectId()))) {
													subGraph1.addObjectOnEdge(currentEdgeId, rn1);
												}

											}
										}

									}
								}
							}
							subgraph0.printObjectsOnEdges();
							subGraph1.printObjectsOnEdges();

						}
					});

//			JavaPairRDD<Object, Iterable<Tuple5<Object, Object, Object, Double, ArrayList<RoadObject>>>> attribSubgGraphsRDD = jscontext
//					.parallelize(tuplesForSubgraphs).groupBy(x -> x._1())
//					.partitionBy(new CustomPartitioner(CustomPartitionSize));
//
//			attribSubgGraphsRDD.foreachPartition(
//					new VoidFunction<Iterator<Tuple2<Object, Iterable<Tuple5<Object, Object, Object, Double, ArrayList<RoadObject>>>>>>() {
//
//						@Override
//						public void call(
//								Iterator<Tuple2<Object, Iterable<Tuple5<Object, Object, Object, Double, ArrayList<RoadObject>>>>> tupleIterator)
//								throws Exception {
//
//							while (tupleIterator.hasNext()) {
//
//								if (Integer.parseInt(String.valueOf(tupleIterator.next()._1())) == 1) {
//									System.out.println(tupleIterator.next()._1());
//								}
//
//							}
//							// TODO Auto-generated method stub
//
//						}
//					});

//			adjVertForSubgraphsRDD.foreachPartition(
//					new VoidFunction<Iterator<Tuple2<Object, Map<Object, Map<Object, Tuple2<Double, ArrayList<RoadObject>>>>>>>() {
//
//						/**
//						 * 
//						 */
//						private static final long serialVersionUID = 1L;
//
//						@Override
//						public void call(
//								Iterator<Tuple2<Object, Map<Object, Map<Object, Tuple2<Double, ArrayList<RoadObject>>>>>> tupleRow)
//								throws Exception {
//							int sourceVertex;
//							int destVertex;
//							double edgeLength;
//
//							int roadObjectId;
//							boolean roadObjectType;
//							double distanceFromStart;
//
//							CoreGraph subgraph0 = new CoreGraph();
//							CoreGraph subgraph1 = new CoreGraph();
//							ArrayList<Integer> addedObjects = new ArrayList<Integer>();
//
//							
//
//							while (tupleRow.hasNext()) {
//
//								Map<Object, Map<Object, Tuple2<Double, ArrayList<RoadObject>>>> newMap = tupleRow.next()
//										._2();
//
//								if ((Integer.parseInt(String.valueOf(tupleRow.next()._1())) == 0)) {
//
//									for (Object srcVertex : newMap.keySet()) {
//
//										for (Object dstVertex : newMap.get(srcVertex).keySet()) {
//
//											sourceVertex = Integer.parseInt(String.valueOf(srcVertex));
//											destVertex = Integer.parseInt(String.valueOf(dstVertex));
//											edgeLength = newMap.get(srcVertex).get(dstVertex)._1();
//
//											subgraph0.addEdge(sourceVertex, destVertex, edgeLength);
//											int currentEdgeId = subgraph0.getEdgeId(sourceVertex, destVertex);
//
//											if (newMap.get(srcVertex).get(dstVertex)._2() != null) {
//
//												for (int i = 0; i < newMap.get(srcVertex).get(dstVertex)._2()
//														.size(); i++) {
//
//													roadObjectId = newMap.get(srcVertex).get(dstVertex)._2().get(i)
//															.getObjectId();
//													roadObjectType = newMap.get(srcVertex).get(dstVertex)._2().get(i)
//															.getType();
//													distanceFromStart = newMap.get(srcVertex).get(dstVertex)._2().get(i)
//															.getDistanceFromStartNode();
//													RoadObject rn0 = new RoadObject();
//													rn0.setObjId(roadObjectId);
//													rn0.setType(roadObjectType);
//													rn0.setDistanceFromStartNode(distanceFromStart);
//
//													if (addedObjects.isEmpty()) {
//														subgraph0.addObjectOnEdge(currentEdgeId, rn0);
////														System.out.println(
////																rn0.getObjectId() + " added on edge: " + currentEdgeId);
//														addedObjects.add(rn0.getObjectId());
//
//													} else if ((!addedObjects.isEmpty())
//															&& (!addedObjects.contains(rn0.getObjectId()))) {
//														subgraph0.addObjectOnEdge(currentEdgeId, rn0);
////														System.out.println(
////																rn0.getObjectId() + " added on edge: " + currentEdgeId);
//														addedObjects.add(rn0.getObjectId());
//
//													}
//
//												}
//											}
//
//										}
//									}
//
//								} else if ((Integer.parseInt(String.valueOf(tupleRow.next()._1())) == 1)) {
//
//									for (Object srcVertex : newMap.keySet()) {
//										for (Object dstVertex : newMap.get(srcVertex).keySet()) {
//
//											sourceVertex = Integer.parseInt(String.valueOf(srcVertex));
//											destVertex = Integer.parseInt(String.valueOf(dstVertex));
//											edgeLength = newMap.get(srcVertex).get(dstVertex)._1();
//
//											subgraph1.addEdge(sourceVertex, destVertex, edgeLength);
//											int currentEdgeId = subgraph1.getEdgeId(sourceVertex, destVertex);
//
//											if (newMap.get(srcVertex).get(dstVertex)._2() != null) {
//												for (int i = 0; i < newMap.get(srcVertex).get(dstVertex)._2()
//														.size(); i++) {
//
//													roadObjectId = newMap.get(srcVertex).get(dstVertex)._2().get(i)
//															.getObjectId();
//													roadObjectType = newMap.get(srcVertex).get(dstVertex)._2().get(i)
//															.getType();
//													distanceFromStart = newMap.get(srcVertex).get(dstVertex)._2().get(i)
//															.getDistanceFromStartNode();
//													RoadObject rn1 = new RoadObject();
//													rn1.setObjId(roadObjectId);
//													rn1.setType(roadObjectType);
//													rn1.setDistanceFromStartNode(distanceFromStart);
//
//													if (addedObjects.isEmpty()) {
//														subgraph1.addObjectOnEdge(currentEdgeId, rn1);
////														System.err.println(
////																rn1.getObjectId() + " added on edge: " + currentEdgeId);
//														addedObjects.add(rn1.getObjectId());
//													} else if ((!addedObjects.isEmpty())
//															&& (!addedObjects.contains(rn1.getObjectId()))) {
//														subgraph1.addObjectOnEdge(currentEdgeId, rn1);
////														System.err.println(
////																rn1.getObjectId() + " added on edge: " + currentEdgeId);
//														addedObjects.add(rn1.getObjectId());
//
//													}
//
//												}
//											}
//
//										}
//									}
//								}
//
//							}
//
//							// subgraph0.printObjectsOnEdges();
//							// subgraph1.printObjectsOnEdges();
//
////							// Straight forward nearest neighbor algorithm from each true to false.
////							ANNNaive ann = new ANNNaive();
////							System.err.println("-------------------------------");
////							Map<Integer, Integer> nearestNeighorPairsSubg0 = ann.compute(subgraph0, true);
////							System.out.println("for subgraph0");
////							System.out.println(nearestNeighorPairsSubg0);
////							System.err.println("-------------------------------");
////
////							System.err.println("-------------------------------");
////							Map<Integer, Integer> nearestNeighorPairsSubg1 = ann.compute(subgraph1, true);
////							System.out.println("for subgraph1");
////							System.out.println(nearestNeighorPairsSubg1);
////							System.err.println("-------------------------------");
//
//						}
//
//					});

//			adjVertForSubgraphsRDD.foreachPartition(
//					new VoidFunction<Iterator<Tuple2<Object, Iterable<Map<Object, Map<Object, Tuple2<Double, ArrayList<RoadObject>>>>>>>>() {
//
//						/**
//						 * 
//						 */
//						private static final long serialVersionUID = -8802441176347993738L;
//
//						@Override
//						public void call(
//								Iterator<Tuple2<Object, Iterable<Map<Object, Map<Object, Tuple2<Double, ArrayList<RoadObject>>>>>>> eachTuple)
//								throws Exception {
//
//							int sourceVertex;
//							int destVertex;
//							double edgeLength;
//
//							int roadObjectId;
//							boolean roadObjectType;
//							double distanceFromStart;
//
//							CoreGraph subgraph0 = new CoreGraph();
//							CoreGraph subgraph1 = new CoreGraph();
//							CoreGraph subgraph = new CoreGraph();
//							ArrayList<Integer> addedObjects = new ArrayList<Integer>();
//
//							while (eachTuple.hasNext()) {
//
//								System.out.println("hello " + eachTuple.next()._1());
//								for (Map<Object, Map<Object, Tuple2<Double, ArrayList<RoadObject>>>> entryMap : eachTuple
//										.next()._2()) {
//
//									// System.out.println("EntryMap: " + eachTuple.next()._1() + " " + entryMap);
//									for (Object sourceKey : entryMap.keySet()) {
//										// System.err.println(entryMap.get(sourceKey));
//										for (Object destKey : entryMap.get(sourceKey).keySet()) {
//											if ((entryMap.get(sourceKey).get(destKey)._2()) != null) {
////												System.out.println(sourceKey + " --> " + destKey + ": "
////														+ entryMap.get(sourceKey).get(destKey)._2());
//
//												sourceVertex = Integer.parseInt(String.valueOf(sourceKey));
//												destVertex = Integer.parseInt(String.valueOf(destKey));
//												edgeLength = entryMap.get(sourceKey).get(destKey)._1();
//
//												subgraph.addEdge(sourceVertex, destVertex, edgeLength);
//
//												for (int i = 0; i < entryMap.get(sourceKey).get(destKey)._2()
//														.size(); i++) {
//													int currentEdgeId = subgraph.getEdgeId(sourceVertex, destVertex);
//
//													roadObjectId = entryMap.get(sourceKey).get(destKey)._2().get(i)
//															.getObjectId();
//													roadObjectType = entryMap.get(sourceKey).get(destKey)._2().get(i)
//															.getType();
//													distanceFromStart = entryMap.get(sourceKey).get(destKey)._2().get(i)
//															.getDistanceFromStartNode();
//													RoadObject rn = new RoadObject();
//													rn.setObjId(roadObjectId);
//													rn.setType(roadObjectType);
//													rn.setDistanceFromStartNode(distanceFromStart);
//
//													subgraph.addObjectOnEdge(currentEdgeId, rn);
//												}
//
//											} else {
//												sourceVertex = Integer.parseInt(String.valueOf(sourceKey));
//												destVertex = Integer.parseInt(String.valueOf(destKey));
//												edgeLength = entryMap.get(sourceKey).get(destKey)._1();
//
//												subgraph.addEdge(sourceVertex, destVertex, edgeLength);
//
//											}
//
//										}
//									}
//
//								}
//								// subgraph.printEdgesInfo();
//
//							}
//
//						}
//
//					});
			jscontext.close();

		}

	}

	public static List<Tuple2<Object, Map<Object, Double>>> createEmbeddedNetwork(JavaRDD<String> BoundaryVerticesRDD) {
		Object virtualVertex = Integer.MAX_VALUE;
		List<Tuple2<Object, Map<Object, Double>>> embeddedNetwork = new ArrayList<>();
		for (Object BoundaryVertex : BoundaryVerticesRDD.collect()) {
			Map<Object, Double> connectingVertex = new HashMap<Object, Double>();
			connectingVertex.put(BoundaryVertex, 0.0);
			embeddedNetwork.add(new Tuple2<Object, Map<Object, Double>>(virtualVertex, connectingVertex));

		}

		return embeddedNetwork;

	}

	public static ArrayList<List<Path>> runSPF(YenGraph yenG, HashMap<Integer, ArrayList<String>> boundaries,
			int partSize) {

		ArrayList<List<Path>> SPList = new ArrayList<List<Path>>();
		Yen yenAlgo = new Yen();

		for (Integer mapIndex : boundaries.keySet()) {
			for (int i = 0; i < boundaries.get(mapIndex).size(); i++) {
				String vertexSrc = boundaries.get(mapIndex).get(i);

				for (int j = mapIndex + 1; j < partSize; j++) {
					for (int k = 0; k < boundaries.get(j).size(); k++) {
						String vertexDest = boundaries.get(j).get(k);
						List<Path> pathList = new LinkedList<Path>();
						pathList = yenAlgo.ksp(yenG, vertexSrc, vertexDest, 1);
						SPList.add(pathList);
						// 09System.out.println(vertexSrc + " -> " + vertexDest);

					}
				}

			}
		}

		return SPList;
	}

	public static ArrayList<List<Path>> runSP(YenGraph yenG, LinkedList<String> boundaryVertices, int partSize) {
		ArrayList<List<Path>> SPList = new ArrayList<List<Path>>();
		Yen yenAlgo = new Yen();

		for (int i = 0; i < boundaryVertices.size(); i++) {
			String srcVertex = boundaryVertices.get(i);
			for (int j = i + 1; j < boundaryVertices.size(); j++) {
				String destVertex = boundaryVertices.get(j);
				List<Path> pathList = new LinkedList<Path>();
				pathList = yenAlgo.ksp(yenG, srcVertex, destVertex, 1);
				SPList.add(pathList);
				// System.out.println(srcVertex + " , " + destVertex);
			}
		}
		return SPList;

	}

	public static List<Path> getSPFbetweenTwoNodes(YenGraph yg, String src, String dest, int partSize) {
		List<Path> spf = new ArrayList<Path>();
		Yen yalg = new Yen();

		spf = yalg.ksp(yg, src, dest, partSize);

		return spf;
	}

	public static LinkedList<Integer> unifyAllShortestPaths(ArrayList<List<Path>> spfList) {
		LinkedList<Integer> shortestpathsUnion = new LinkedList<Integer>();

		for (List<Path> p : spfList) {
			for (int i = 0; i < p.size(); i++) {
				for (int j = 0; j < p.get(i).getNodes().size(); j++) {
					int vertex = Integer.parseInt(p.get(i).getNodes().get(j));
					if (!shortestpathsUnion.contains(vertex)) {

						shortestpathsUnion.add(vertex);
						// System.out.println("added " + vertex);
					}

				}
				// System.out.println(" ");
			}
		}

		return shortestpathsUnion;
	}

	public static CoreGraph readSPFreturnGraph(ArrayList<List<Path>> shortestPathList) {

		CoreGraph embeddedGraph = new CoreGraph();

		for (List<Path> path : shortestPathList) {
			for (Path p : path) {
				int srcVertex = Integer.parseInt(p.getEdges().getFirst().getFromNode());
				int destinationVertex = Integer.parseInt(p.getEdges().getLast().getToNode());
				double edgeWeight = Double.valueOf(p.getTotalCost());
				embeddedGraph.addEdge(srcVertex, destinationVertex, edgeWeight);

			}
		}

		return embeddedGraph;

	}

	public static Tuple2<Integer, Integer> getNearestDataObject() {
		Tuple2<Integer, Integer> NNs = null;

		return NNs;
	}

	/**
	 * ending bracket
	 * 
	 */
}