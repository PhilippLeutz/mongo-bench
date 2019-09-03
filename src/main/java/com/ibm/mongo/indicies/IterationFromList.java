package com.ibm.mongo.indicies;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IterationFromList implements IndicieGenerator {
	static final Logger LOG = LoggerFactory.getLogger(IterationFromList.class);

	private ArrayList<Integer> validIndices;
	private AtomicInteger nextElement = new AtomicInteger(0);

	IterationFromList(String indiciesPath) throws IOException {
		this.validIndices = IndiciesUtil.generateValidIndicies(indiciesPath);
	}

	IterationFromList(ArrayList<Integer> validIndicies) {
		this.validIndices = validIndicies;
	}

	int getIndexSize(){
		return validIndices.size();
	}

	@Override
	public int getNextIndex() {
		int currentIndexNumber = Math.abs(nextElement.getAndIncrement()%validIndices.size());
		int index = validIndices.get(currentIndexNumber);
		return index;
	}
}
