package com.aavash.ann.pregel;

import scala.Serializable;
import scala.runtime.AbstractFunction3;

public class VprogD extends AbstractFunction3<Long, Double, Double, Double> implements Serializable {

	static final Double INITIAL_VALUE = Double.MAX_VALUE;

	@Override
	public Double apply(Long vertexId, Double vertexValue, Double message) {
		// TODO Auto-generated method stub

		if (message.equals(INITIAL_VALUE)) {
			return vertexValue;
		} else {
			return Math.min(vertexValue, message);
		}

	}

}
