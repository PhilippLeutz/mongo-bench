package com.ibm.mongo;

import java.util.Random;

import org.apache.commons.lang.RandomStringUtils;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibm.mongo.LoadThread.DocType;
import com.thedeanda.lorem.LoremIpsum;

public class DataGeneration {
	private static final Logger log = LoggerFactory.getLogger(LoadThread.class);
	private static Random randomNumbers = new Random();
	
	static public Document[] createDocuments(int count, DocType docType, int startRecord, int docsize) {
		final Document[] docs = new Document[count];

			final String data = generateData(docType, docsize);
			for (int i = 0; i < count; i++) {
				docs[i] = new Document()
				.append("_id", i + startRecord)
				.append("data", data);
			}	
		log.info("New data created with ids {} to {}", startRecord, startRecord+count-1 );
		return docs;
	}
	
	static public Document createSingleDocument(DocType docType, int iD, int docsize){
		final String data = generateData(docType, docsize);
		Document document = new Document()
			.append("_id", iD)
			.append("data", data);
		return document;
	}

	public static String generateData(DocType docType, int docSize) {
		String data = "";
		if(docType.equals(DocType.lorem)){
			data=generateLoremData(docSize);
		}
		else{
			data = RandomStringUtils.randomAlphabetic(docSize);
		}
		return data;
	}

	private static String generateLoremData(int docSize) {
		String sentence = new LoremIpsum(randomNumbers.nextLong()).getWords(docSize);
		return sentence.substring(0,docSize);
	}
}
