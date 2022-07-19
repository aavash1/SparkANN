package com.ann.sparkann.framework;

import java.util.Comparator;

import scala.Serializable;

public class Node implements Serializable {
	private int m_intNodeId;
	private Double m_doubLatitude;
	private Double m_doubLongitude;

	public Node(int nodeId, double longitude, double latitude) {
		// TODO Auto-generated constructor stub
		this.m_intNodeId = nodeId;
		this.m_doubLatitude = longitude;
		this.m_doubLongitude = latitude;
	}

	public Node() {

	}

	public int getNodeId() {
		return m_intNodeId;
	}

	public void setNodeId(int strNodeId) {
		this.m_intNodeId = strNodeId;
	}

	public Double getLongitude() {
		return m_doubLongitude;
	}

	public void setLongitude(Double doubLongitude) {
		this.m_doubLongitude = doubLongitude;
	}

	public Double getLatitude() {
		return m_doubLatitude;
	}

	public void setLatitude(Double doubLatitude) {
		this.m_doubLatitude = doubLatitude;
	}

	@Override
	public String toString() {
		return "Node [NodeId=" + m_intNodeId + ", " + "Latitude=" + m_doubLatitude + " Longitude=" + m_doubLongitude
				+ "]";
	}

}
