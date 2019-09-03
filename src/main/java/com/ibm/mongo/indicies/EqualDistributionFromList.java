package com.ibm.mongo.indicies;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class EqualDistributionFromList implements IndicieGenerator {

	private Random random;
	private ArrayList<Integer> validIndicies;
	private static final Logger LOG = LoggerFactory.getLogger(EqualDistributionFromList.class);

	EqualDistributionFromList(String limitspath) throws IOException, ParseException {
		random = new Random();
		validIndicies = new ArrayList<>();
		ArrayList<Integer> lowerRanges = new ArrayList<>();
		ArrayList<Integer> upperRanges = new ArrayList<>();

		List<String> limitLines = Files.readAllLines(Paths.get(limitspath), Charset.defaultCharset());
		
		for(String currentLine:limitLines){
			String[] splittedLine = currentLine.split(",");
			lowerRanges.add(Integer.parseInt(splittedLine[0]));
			upperRanges.add(Integer.parseInt(splittedLine[1]));
		}

		for(int currentRange = 0; currentRange < lowerRanges.size(); currentRange++){
			LOG.info("Add indicies {} to {} as valid indicies", lowerRanges.get(currentRange),  upperRanges.get(currentRange));
			for(int currentIndex = lowerRanges.get(currentRange); currentIndex < upperRanges.get(currentRange); currentIndex++){
				validIndicies.add(currentIndex);
			}
		}
	}

	/* (non-Javadoc)
	 * @see com.ibm.mongo.IndicieGenerator#getNextIndex()
	 */
	@Override
	public int getNextIndex(){
		return (int)validIndicies.get(random.nextInt(validIndicies.size()));
	}

}
