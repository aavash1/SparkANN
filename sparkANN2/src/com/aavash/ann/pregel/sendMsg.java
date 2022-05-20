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

public class sendMsg extends AbstractFunction1<EdgeTriplet<Integer, Integer>, Iterator<Tuple2<Object, Integer>>>
		implements Serializable {

	static final Integer INITIAL_VALUE = Integer.MAX_VALUE;

	@Override
	public Iterator<Tuple2<Object, Integer>> apply(EdgeTriplet<Integer, Integer> triplet) {
		// TODO Auto-generated method stub

		Long srcId = triplet.srcId();
		Long dstId = triplet.dstId();
		Integer weight = triplet.attr();
		Integer srcVertex = triplet.srcAttr();
		Integer descVertex = triplet.dstAttr();

		if (srcVertex.equals(INITIAL_VALUE)) {
			return JavaConverters.asScalaIteratorConverter(new ArrayList<Tuple2<Object, Integer>>().iterator())
					.asScala();
		} else {
			int value_to_send = srcVertex + weight;
			return JavaConverters
					.asScalaIteratorConverter(
							Arrays.asList(new Tuple2<Object, Integer>(triplet.dstId(), value_to_send)).iterator())
					.asScala();
		}

	}

}
