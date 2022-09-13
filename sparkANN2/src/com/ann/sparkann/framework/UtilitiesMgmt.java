package com.ann.sparkann.framework;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.aavash.ann.sparkann.graph.Vertices;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import org.apache.spark.graphx.Edge;
import org.apache.spark.graphx.Graph;

import scala.Tuple2;

public class UtilitiesMgmt {
	final static String txtSplitBy = " ";

	public static List<Edge<Double>> readTextEdgeFile(List<Edge<Double>> edgeList, String txtFileName)
			throws FileNotFoundException, IOException {
		String line = "";
		String txtSplitBy = "  ";
		boolean removedBOM = false;
		try (BufferedReader br = new BufferedReader(new FileReader(txtFileName))) {
			while ((line = br.readLine()) != null) {
				String[] record = line.split(txtSplitBy);
				if (record.length == 4) {
					if (!removedBOM && record[0] != "0") {

						record[0] = String.valueOf(0);
						removedBOM = true;

					}

					edgeList.add(new Edge<Double>(Integer.parseInt(record[1]), Integer.parseInt(record[2]),
							Double.parseDouble(record[3])));

				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

		return edgeList;

	}

	public static List<Tuple2<Object, Vertices>> readTextNodeFile(List<Tuple2<Object, Vertices>> verticesList,
			String txtFileName) throws FileNotFoundException, IOException {
		String line = "";
		String txtSplitBy = " ";
		boolean removedBOM = false;
		long counter = 0L;
		try (BufferedReader br = new BufferedReader(new FileReader(txtFileName))) {
			while ((line = br.readLine()) != null) {
				String[] record = line.split(txtSplitBy);
				if (record.length == 4) {
					if (!removedBOM && record[0] != "0") {

						record[0] = String.valueOf(0);
						removedBOM = true;

					}
					verticesList.add(new Tuple2<Object, Vertices>(counter, new Vertices(Integer.parseInt(record[0]),
							Double.parseDouble(record[1]), Double.parseDouble(record[2]))));
					counter++;

				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

		return verticesList;

	}

	public static List<Tuple2<Object, String>> readTextNodeFile1(List<Tuple2<Object, String>> nodeList,
			String txtFileName) throws FileNotFoundException, IOException {
		String line = "";
		String txtSplitBy = " ";
		boolean removedBOM = false;
		long counter = 0L;
		try (BufferedReader br = new BufferedReader(new FileReader(txtFileName))) {
			while ((line = br.readLine()) != null) {
				String[] record = line.split(txtSplitBy);
				if (record.length == 4) {
					if (!removedBOM && record[0] != "0") {

						record[0] = String.valueOf(0);
						removedBOM = true;

					}
					nodeList.add(new Tuple2<Object, String>(counter, record[0]));
					counter++;

				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

		return nodeList;

	}

	public static ImmutableMap<Long, Integer> readTextNodeReturnImtmap(String txtFileName) {
		Map<Long, Integer> map = new HashMap<>();
		Long counter = 1L;

		String line = "";
		String txtSplitBy = " ";
		boolean removedBOM = false;
		try (BufferedReader br = new BufferedReader(new FileReader(txtFileName))) {
			while ((line = br.readLine()) != null) {
				String[] record = line.split(txtSplitBy);
				if (record.length == 3) {
//					if (!removedBOM && record[0] != "0") {
//
//						record[0] = String.valueOf(0);
//						removedBOM = true;
//
//					}
					map.put(counter, Integer.parseInt(record[0]));
					counter++;

				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

		ImmutableMap<Long, Integer> toReturn = ImmutableMap.<Long, Integer>builder().putAll(map).build();

		return toReturn;

	}

	public static List<Tuple2<Object, Double>> getMapKeysCreateList(Map<Long, Integer> inputMap) {
		List<Tuple2<Object, Double>> vertices = new ArrayList<Tuple2<Object, Double>>();

		for (Long key : inputMap.keySet()) {
			if (key == 1l) {
				vertices.add(new Tuple2<Object, Double>(key, 0.0));
			} else {
				vertices.add(new Tuple2<Object, Double>(key, Double.MAX_VALUE));
			}

		}

		return vertices;
	}

	public static List<Edge<Double>> readTextEdgeFileD(String txtFileName) throws FileNotFoundException, IOException {
		String line = "";
		String txtSplitBy = " ";
		boolean removedBOM = false;
		List<Edge<Double>> edgeList = new ArrayList<Edge<Double>>();
		try (BufferedReader br = new BufferedReader(new FileReader(txtFileName))) {
			while ((line = br.readLine()) != null) {
				String[] record = line.split(txtSplitBy);
				if (record.length == 4) {
					if (!removedBOM && record[0] != "0") {

						// record[0] = String.valueOf(0);
						removedBOM = true;

					}

					edgeList.add(new Edge<Double>(Long.valueOf(record[1]), Long.valueOf(record[2]),
							Double.parseDouble(record[3])));

				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

		return edgeList;

	}

	public Graph<String, Double> getSubgraphAfterPartition(Graph<String, Double> inputGraph, int numberOfPartition) {

		return inputGraph;
	}

	// Read the text file and return the immutable map of

	// txtFileName1: partition file
	// txtFileName2: Node file
	public static void readMultipleTextFiles(String txtFileName1, String txtFileName2,
			List<Map<Integer, Integer>> toPartition) throws IOException {

		String txtSplitBy = " ";
		boolean removedBOM = false;
		File[] files = { new File(txtFileName1), new File(txtFileName2) };
		// fetching all files
		for (File file : files) {
			BufferedReader inputStream = null;
			String line;
			try {
				inputStream = new BufferedReader(new FileReader(file));
				while ((line = inputStream.readLine()) != null) {

				}

			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				if (inputStream != null) {
					inputStream.close();
				}
			}
		}

	}

	public static void writeMETISoutputFile(CoreGraph graph, String outputFileName, int NumberOfEdge,
			int NumberOfVertices) {

	}

	public static void writeHGREdgeFile(Graph<String, Double> graph, int NumberOfEdge, int NumberOfVertices,
			String inputFileName, String outputFileName, boolean fmt) {
		String line = "";
		String txtSplitBy = " ";
		boolean removedBOM = false;
		long counter = 0L;
		int fmtValue = 0;
		// Number of Edges|Number of Vertices|Fmt (yes=1, no=0)

		if (fmt == false) {
			try (BufferedReader br = new BufferedReader(new FileReader(inputFileName))) {
				FileWriter outputFile = new FileWriter(outputFileName, true);
				outputFile.write(String.format(NumberOfEdge + txtSplitBy + NumberOfVertices));
				outputFile.write(System.lineSeparator());
				while ((line = br.readLine()) != null) {
					String[] record = line.split(txtSplitBy);
					if (record.length == 4) {
						if (!removedBOM && record[0] != "0") {

							record[0] = String.valueOf(0);
							removedBOM = true;

						}

						outputFile.write(String.format(record[1] + txtSplitBy + record[2]));
						outputFile.write(System.lineSeparator());

					}
				}

				outputFile.close();
			} catch (IOException e) {
				e.printStackTrace();
			}

		} else {
			try (BufferedReader br = new BufferedReader(new FileReader(inputFileName))) {
				FileWriter outputFile = new FileWriter(outputFileName, true);
				outputFile.write(String.format(NumberOfEdge + txtSplitBy + NumberOfVertices));
				outputFile.write(System.lineSeparator());
				while ((line = br.readLine()) != null) {
					String[] record = line.split(txtSplitBy);
					if (record.length == 4) {
						if (!removedBOM && record[0] != "0") {

							record[0] = String.valueOf(0);
							removedBOM = true;

						}
						outputFile.write(String.format(record[3] + txtSplitBy + record[1] + txtSplitBy + record[2]));
						outputFile.write(System.lineSeparator());

					}
				}

				outputFile.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		System.out.println("File Written Successfully");

	}

	// Read METIS Input type
	public static Map<Integer, List<Integer>> readMETISInputGraph(String metisInput,
			Map<Integer, List<Integer>> metisHolder) {
		String line = "";
		String txtSplitBy = " ";
//		boolean removedBOM = false;
		long counter = 1L;

		try (BufferedReader br = new BufferedReader(new FileReader(metisInput))) {
			// System.out.println("Inside the reader");
			br.readLine();
			while ((line = br.readLine()) != null) {
				String[] record = line.split(txtSplitBy);
				int textLength = line.split(txtSplitBy).length;
				List<Integer> adjacentNeighbors = new ArrayList<Integer>();
				for (int i = 0; i < textLength; i++) {
					if (record[i] != "") {
						adjacentNeighbors.add(Integer.parseInt(record[i]));

					} else {
						adjacentNeighbors.add(Integer.parseInt(record[i + 1]));
					}

				}
				metisHolder.put((int) counter, adjacentNeighbors);
				counter++;

			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		// System.out.println("Reading successful");
		return metisHolder;

	}

	// Read METIS output file
	public static ArrayList<Integer> readMETISPartition(String partitionFile, ArrayList<Integer> partitonIndex)
			throws IOException, IOException {
		String line = "";
		String txtSplitBy = "\\r?\\n";
		try (BufferedReader br = new BufferedReader(new FileReader(partitionFile))) {
			while ((line = br.readLine()) != null) {
				String[] record = line.split(txtSplitBy);

				partitonIndex.add(Integer.parseInt(record[0]));

			}

		} catch (IOException e) {
			e.printStackTrace();
		}
		return partitonIndex;
	}

	// Sort the HashMap by values using java 8 lambdas.
	public static Map<Map<Object, List<Integer>>, Integer> sortByValue(Map<Map<Object, List<Integer>>, Integer> hm) {

		// Create a list from elements of map
		List<Map.Entry<Map<Object, List<Integer>>, Integer>> list = new LinkedList<Map.Entry<Map<Object, List<Integer>>, Integer>>(
				hm.entrySet());

		// sort the list using lambda expression
		Collections.sort(list, (i1, i2) -> i1.getValue().compareTo(i2.getValue()));

		// put the data from sorted list to hashmap
		Map<Map<Object, List<Integer>>, Integer> temp = new LinkedHashMap<Map<Object, List<Integer>>, Integer>();
		for (Map.Entry<Map<Object, List<Integer>>, Integer> aa : list) {
			temp.put(aa.getKey(), aa.getValue());

		}
		return temp;

	}

}
