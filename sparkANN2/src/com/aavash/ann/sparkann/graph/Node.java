package com.aavash.ann.sparkann.graph;

public class Node {
	private int Node_Id;
	private double longitude;
	private double latitude;

	public int getNode_Id() {
		return Node_Id;
	}

	public void setNode_Id(int node_Id) {
		Node_Id = node_Id;
	}

	public double getLongitude() {
		return longitude;
	}

	public void setLongitude(double longitude) {
		this.longitude = longitude;
	}

	public double getLatitude() {
		return latitude;
	}

	public void setLatitude(double latitude) {
		this.latitude = latitude;
	}

	@Override
	public String toString() {
		return "Node Id: " + getNode_Id() + " Longitude: " + getLongitude() + " Latitude: " + getLatitude();
	}
}
