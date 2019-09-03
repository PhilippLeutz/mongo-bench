package com.ibm.mongo.indicies;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class IndiciesUtil {

	static ArrayList<Integer> generateValidIndicies(String indiciesPath) throws IOException {
		ArrayList<Integer> lowerRanges = new ArrayList<>();
		ArrayList<Integer> upperRanges = new ArrayList<>();
		ArrayList<Integer> validIndicies = new ArrayList<>();
		
		List<String> limitLines = Files.readAllLines(Paths.get(indiciesPath), Charset.defaultCharset());
		
		for(String currentLine:limitLines){
			String[] splittedLine = currentLine.split(",");
			lowerRanges.add(Integer.parseInt(splittedLine[0]));
			upperRanges.add(Integer.parseInt(splittedLine[1]));
		}
	
		for(int currentRange = 0; currentRange < lowerRanges.size(); currentRange++){
			EqualDistribution.LOG.info("Add indicies {} to {} as valid indicies", lowerRanges.get(currentRange),  upperRanges.get(currentRange));
			for(int currentIndex = lowerRanges.get(currentRange); currentIndex < upperRanges.get(currentRange); currentIndex++){
				validIndicies.add(currentIndex);
			}
		}
		return validIndicies;
	}

}
