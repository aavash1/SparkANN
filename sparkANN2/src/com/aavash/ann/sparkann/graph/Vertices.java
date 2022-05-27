package com.aavash.ann.sparkann.graph;

import java.io.Serializable;

public class Vertices implements Serializable {
	private int vertex_Id;
	private double v_longitude;
	private double v_latitude;
	public int borderNode_Id;
	private boolean isBorderNode;

	public Vertices(int nodeId, double longitude, double latitude) {
		// TODO Auto-generated constructor stub
		this.vertex_Id = nodeId;
		this.v_longitude = longitude;
		this.v_latitude = latitude;
	}

	public Vertices() {

	}

	public int getNode_Id() {
		return vertex_Id;
	}

	public void setNode_Id(int node_Id) {
		vertex_Id = node_Id;
	}

	public double getLongitude() {
		return v_longitude;
	}

	public void setLongitude(double longitude) {
		this.v_longitude = longitude;
	}

	public double getLatitude() {
		return v_latitude;
	}

	public void setLatitude(double latitude) {
		this.v_latitude = latitude;
	}

	public void setBorderNodeId(int borderNodeId) {
		this.borderNode_Id = borderNodeId;
	}

	public int getBorderNodeId() {
		return borderNode_Id;
	}

	public boolean checkIfIsBorder(int nodeId) {
		return false;

	}

	@Override
	public String toString() {
		return "Node Id: " + getNode_Id() + " Longitude: " + getLongitude() + " Latitude: " + getLatitude();
	}
}
