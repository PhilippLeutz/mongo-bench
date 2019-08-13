package com.ibm.mongo;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.commons.cli.ParseException;


public class IndiciesHelper {

	private Random random;
	private ArrayList<Integer> validIndicies;

	public IndiciesHelper(int numThreads, String limitspath, int numDocuments) throws IOException, ParseException {
		random = new Random();
		validIndicies = new ArrayList<>();
		ArrayList<Integer> limitsList = new ArrayList<>();
		List<String> limitLines = Files.readAllLines(Paths.get(limitspath), Charset.defaultCharset());
		if(limitLines.size() != numThreads){
			throw new ParseException("Number of Index limits unequal to number of Threads");
		}
		
		for(String currentLine:limitLines){
			limitsList.add(Integer.parseInt(currentLine));
		}
		
		Integer[] offsetForThreads = new Integer[limitsList.size()];
		limitsList.toArray(offsetForThreads);
		
		int documentsPerThread = numDocuments/numThreads;

		int currentStartIndex=0;
		for(int currentThreadIndex = 0; currentThreadIndex < numThreads; currentThreadIndex++){
			for(int currentIndex = currentStartIndex; currentIndex <= offsetForThreads[currentThreadIndex]; currentIndex++){
				validIndicies.add(currentIndex);
			}
			currentStartIndex = currentStartIndex+documentsPerThread;
		}
	}

	public IndiciesHelper(int numThreads, int numDocuments) {

		for(int currentIndex = 0; currentIndex < numDocuments; currentIndex++){
			validIndicies.add(currentIndex);
		}
		return;
	}

	public int getNextIndex(){
		return (int)validIndicies.get(random.nextInt(validIndicies.size()));
	}

	public ArrayList<Integer> getValidIndicies() {
		return validIndicies;
	}

}
