package com.aavash.ann.sparkann.graph;

import java.io.Serializable;

public class EdgeNetwork implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private int edge_id;
	private int source_id;
	private int destination_id;
	private double edge_length;

	public int getEdge_id() {
		return edge_id;
	}

	public void setEdge_id(int edge_id) {
		this.edge_id = edge_id;
	}

	public int getSource_id() {
		return source_id;
	}

	public void setSource_id(int source_id) {
		this.source_id = source_id;
	}

	public int getDestination_id() {
		return destination_id;
	}

	public void setDestination_id(int destination_id) {
		this.destination_id = destination_id;
	}

	public double getEdge_length() {
		return edge_length;
	}

	public void setEdge_length(double edge_length) {
		this.edge_length = edge_length;
	}

	@Override
	public String toString() {
		return "Edge ID:" + edge_id + " Source Id: " + source_id + " Destination Id: " + destination_id
				+ " Edge Length: " + edge_length;
	}

}
