package com.aavash.ann.sparkann.graph;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.graphx.Edge;
import org.apache.spark.graphx.Graph;

import scala.Tuple2;

public class Utilsmanagement {
	final static String txtSplitBy = " ";

	public static boolean readTextEdgeFile(List<Edge<Double>> edgeList, String txtFileName)
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

		return true;

	}

	public static boolean readTextNodeFile(List<Tuple2<Object, String>> nodeList, String txtFileName)
			throws FileNotFoundException, IOException {
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

		return true;

	}

	public Graph<String, Double> getSubgraphAfterPartition(Graph<String, Double> inputGraph, int numberOfPartition) {

		return inputGraph;
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

}
