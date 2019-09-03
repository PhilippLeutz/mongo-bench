package com.ibm.mongo.indicies;
import java.io.IOException;

import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibm.mongo.LoadThread;

public class IndiciesHelperFactory {

	private static final Logger log = LoggerFactory.getLogger(LoadThread.class);


	public enum HelperType{
		equaldistribution, iterator;
	}

	public static IndicieGenerator createIndiciesWithIndexLimits(String path, HelperType type) throws IOException, ParseException{
		switch (type) {
		case equaldistribution:
			log.info("Use Index generator of Type " + type);
			return new EqualDistributionFromList(path);
		case iterator:
			log.info("Use Index generator of Type " + type);
			return new IterationFromList(path);
		default:
			break;
		}
		throw new ParseException("Helper Type " + type  + " not available");
	}

	public static IndicieGenerator createIndices(HelperType type, int numberOfDocuments) throws ParseException{
		switch (type) {
		case equaldistribution:
			log.info("Use Index generator of Type " + HelperType.equaldistribution + "");
			return new EqualDistribution(numberOfDocuments);
		case iterator:
			log.info("Use Index generator of Type " + HelperType.equaldistribution + "");
			return new Iteration(numberOfDocuments);
		default:
			throw new ParseException("Helper Type " + type  + " not available");
		}
	}
	
	public static String printHelp(){
		return "Can be '" + HelperType.iterator + "', iterating over all valid indices or '" + HelperType.iterator+ "' choosing indicies randomly and equal distributed. ";
	}
}
