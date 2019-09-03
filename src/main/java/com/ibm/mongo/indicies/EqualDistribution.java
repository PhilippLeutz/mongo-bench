package com.ibm.mongo.indicies;

import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This implementation of the IndiceGenerator, generating indicies approximately equally distributed over the set of valid indicies. 
 * 
 * @author PhilippLeutz
 *
 */
public class EqualDistribution implements IndicieGenerator {

	private Random random;
	static final Logger LOG = LoggerFactory.getLogger(EqualDistribution.class);
	private final int numDocuments;

	public EqualDistribution(int numDocuments) {
		this.random = new Random();
		this.numDocuments = numDocuments;
	}

	/* (non-Javadoc)
	 * @see com.ibm.mongo.IndicieGenerator#getNextIndex()
	 */
	@Override
	public int getNextIndex(){
		return Math.abs(random.nextInt(numDocuments) % numDocuments);
	}

}
