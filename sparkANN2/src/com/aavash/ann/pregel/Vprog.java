package com.aavash.ann.pregel;

import scala.Serializable;
import scala.runtime.AbstractFunction3;

public class Vprog extends AbstractFunction3<Long, Integer, Integer, Integer> implements Serializable {

	static final Integer INITIAL_VALUE = Integer.MAX_VALUE;
	

	@Override
	public Integer apply(Long vertexId, Integer vertexValue, Integer message) {
		// TODO Auto-generated method stub

		if (message.equals(INITIAL_VALUE)) {
			return vertexValue;
		} else {
			return Math.min(vertexValue, message);
		}

	}

}
