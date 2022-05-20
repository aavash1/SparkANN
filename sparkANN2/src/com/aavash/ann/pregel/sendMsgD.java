package com.aavash.ann.pregel;

import java.util.ArrayList;
import java.util.Arrays;

import org.apache.spark.graphx.EdgeTriplet;

import scala.Serializable;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.JavaConverters;
import scala.collection.convert.Decorators.AsScala;
import scala.runtime.AbstractFunction1;

public class sendMsgD extends AbstractFunction1<EdgeTriplet<Double, Double>, Iterator<Tuple2<Object, Double>>>
		implements Serializable {

	static final Double INITIAL_VALUE = Double.MAX_VALUE;

	@Override
	public Iterator<Tuple2<Object, Double>> apply(EdgeTriplet<Double, Double> triplet) {
		// TODO Auto-generated method stub

		Long srcId = triplet.srcId();
		Long dstId = triplet.dstId();
		Double weight = triplet.attr();
		Double srcVertex = triplet.srcAttr();
		Double descVertex = triplet.dstAttr();

		if (srcVertex.equals(INITIAL_VALUE)) {
			return JavaConverters.asScalaIteratorConverter(new ArrayList<Tuple2<Object, Double>>().iterator())
					.asScala();
		} else {
			double value_to_send = srcVertex + weight;
			return JavaConverters
					.asScalaIteratorConverter(
							Arrays.asList(new Tuple2<Object, Double>(triplet.dstId(), value_to_send)).iterator())
					.asScala();
		}

	}

}
