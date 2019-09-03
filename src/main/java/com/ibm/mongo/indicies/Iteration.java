package com.ibm.mongo.indicies;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Iteration implements IndicieGenerator {
	static final Logger LOG = LoggerFactory.getLogger(Iteration.class);

	private ArrayList<Integer> validIndices;
	private AtomicInteger nextElement = new AtomicInteger(0);
	private int indexSize;

	public Iteration(int numberOfDocuments) {
		this.indexSize = numberOfDocuments;
	}

	int getIndexSize(){
		return validIndices.size();
	}

	@Override
	public int getNextIndex() {
		int index = Math.abs(nextElement.getAndIncrement()%indexSize);
		return index;
	}
}
