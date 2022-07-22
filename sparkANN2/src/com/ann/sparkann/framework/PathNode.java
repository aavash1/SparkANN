package com.ann.sparkann.framework;

public class PathNode implements Comparable<PathNode> {

	int vertex;
	int parent;
	double distanceToRoot;

	public PathNode(int vertex, int parent, double distanceToRoot) {
		this.vertex = vertex;
		this.parent = parent;
		this.distanceToRoot = distanceToRoot;
	}

	@Override
	public int compareTo(PathNode another) {
		// TODO Auto-generated method stub
		if (this.distanceToRoot < another.distanceToRoot) {
			return -1;
		} else if (another.distanceToRoot < this.distanceToRoot) {
			return 1;
		}
		return 0;

	}

	@Override
	public String toString() {
		return "( " + this.vertex + ", " + this.parent + ", " + this.distanceToRoot + ")";

	}

}
