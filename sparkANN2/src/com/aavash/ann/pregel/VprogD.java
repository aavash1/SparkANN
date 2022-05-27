package com.aavash.ann.pregel;

import scala.Serializable;
import scala.runtime.AbstractFunction3;

public class VprogD extends AbstractFunction3<Long, Double, Double, Double> implements Serializable {

	static final Double INITIAL_VALUE = Double.MAX_VALUE;

	@Override
	public Double apply(Long vertexId, Double vertexValue, Double message) {
		// TODO Auto-generated method stub

		System.out.println("[ VProg.apply ] vertexID: '" + vertexId + "' vertexValue: '" + vertexValue + "' message: '"
				+ message + "'");

		if (message.equals(INITIAL_VALUE)) {
			System.out.println("[ VProg.apply ] First superstep -> vertexID: '" + vertexId + "'");
			return vertexValue;
		} else {
			System.out.println("[ VProg.apply ] vertexID: '" + vertexId + "' will send '"
					+ Math.min(vertexValue, message) + "' value");
			return Math.min(vertexValue, message);
		}

	}

}
