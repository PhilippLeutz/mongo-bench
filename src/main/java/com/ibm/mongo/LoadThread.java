/*
 * Copyright (c) 2017, IBM All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package com.ibm.mongo;

import java.io.FileReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.RandomStringUtils;
import org.bson.Document;
import org.json.simple.parser.JSONParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientURI;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.thedeanda.lorem.LoremIpsum;

public class LoadThread implements Runnable {

	private static final Logger log = LoggerFactory.getLogger(LoadThread.class);

	private final int id;
	private final List<String> mongoUri;     // A list with MongoDB URI enteries
	private final int numDocsToInsert;
	private final int docSize;
	private final int maxBatchSize = 1000;
	private final int timeoutMs;
	private final Map<String, Integer> failed = new HashMap<>();
	private final int startRecord;          // Starting insert record number
	private final DocType docType;
	private final String jsonPath;
	private boolean skipDrop;
	
	public enum DocType {
		random, lorem, json,
	}

	public LoadThread(int id, List<String> mongoUri, int numDocuments, int docSize, int timeout, 
			int startRecord, int endRecord, DocType doctype, String jsonPath, boolean skipDrop) {
		this.id = id;
		this.mongoUri = mongoUri;
		this.docSize = docSize;
		this.timeoutMs = timeout * 1000;
		this.startRecord = startRecord;
		this.numDocsToInsert = endRecord - startRecord + 1;
		this.docType = doctype;
		this.jsonPath = jsonPath;
		this.skipDrop = skipDrop;
	}

	@Override
	public void run() {
		log.info("Thread {} loading {} docs into {} instances", id, numDocsToInsert, mongoUri.size());
		for (int i = 0; i < mongoUri.size(); i++) {
			String uri = mongoUri.get(i);
			log.info("Thread {} connnecting to database URI {}", id, uri);

			MongoURI.parseURI(uri); 
			List<String> host = MongoURI.host;
			boolean sslEnabled = MongoURI.isSSLEnabled;

			int count = 0, currentBatchSize;
			final MongoClientOptions ops = MongoClientOptions.builder()
					.maxWaitTime(timeoutMs)
					.connectTimeout(timeoutMs)
					.socketTimeout(timeoutMs)
					.heartbeatConnectTimeout(timeoutMs)
					.serverSelectionTimeout(timeoutMs)
					.sslEnabled(sslEnabled)
					.sslInvalidHostNameAllowed(true)
					.build();

			MongoClientURI cUri = new MongoClientURI(uri, new MongoClientOptions.Builder(ops));
			MongoClient client = new MongoClient(cUri);

			if(!skipDrop){
				for (final String name : client.listDatabaseNames()) {
					if (name.equalsIgnoreCase(MongoBench.DB_NAME)) {
						log.warn("Database {} exists and will be purged before inserting", MongoBench.DB_NAME);
						client.dropDatabase(name);
						break;
					}
				}
			} else {
				log.info("Skip Dropping of Database {} as specified by runtime parameter");
			}
			long startLoad = System.currentTimeMillis();
			while (count < numDocsToInsert) {
				currentBatchSize = numDocsToInsert - count > maxBatchSize ? maxBatchSize : numDocsToInsert - count;
				final Document[] docs = createDocuments(currentBatchSize, count, docType);
				final MongoCollection<Document> mongoCollection = client.getDatabase(MongoBench.DB_NAME).getCollection(MongoBench.COLLECTION_NAME);
				try {
					for(int currentDocument = 0; currentDocument < docs.length; currentDocument++){
						Object iDToFind = docs[currentDocument].get("_id");
						if(CheckIfDocumentExists(iDToFind, mongoCollection)){
							log.info("Document {} already exists in collection, skipping", iDToFind);
							continue;
						}
						mongoCollection.insertOne(docs[currentDocument]);
						if((int) iDToFind % 10 == 0){
							log.info("Thread {} Successfully inserted document {}", id, iDToFind);
						}
					}
				} catch (Exception e) {
					log.error("Error while inserting {} documents at {}", currentBatchSize, host, e);
					log.warn("Checking connection to {}", host);
					boolean connected = false;
					try {
						while (!connected) {
							client.listDatabases();
							connected = true;
							break;
						}
					} catch (Exception ie) {
						log.error("Thread {} no connection to {}. Reconnecting...", id, host);
						client.close();
						client = new MongoClient(cUri);
					}
				}
				count += currentBatchSize;
			}
			client.close();
			long duration = System.currentTimeMillis() - startLoad;
			float rate = numDocsToInsert * 1000f / (float) duration;
			if (failed.size() > 0) {
				int numFailed = 0;
				log.error("Errors occured during the loading of the data");
				for (final Map.Entry<String, Integer> error : failed.entrySet()) {
					log.error("Thread {} unable to insert {} documents at {}:{}", 
							id, error.getValue(), error.getKey());
					numFailed += error.getValue();
				}
				log.error("Thread {} overall {} inserts failed", id, numFailed);
			}
			log.info("Thread {} finished loading {} documents in {} [{} inserts/sec]", 
					id, count, host, rate);
		}
	}

	private boolean CheckIfDocumentExists(Object iDToFind , MongoCollection<Document> mongoCollection) {
		FindIterable<Document> find = mongoCollection.find(new Document("_id",iDToFind));
		return find.first() != null;
	}

	Document[] createDocuments(int count, int offset, DocType docType) {
		final Document[] docs = new Document[count];

		if(docType.equals(DocType.json)){
			for(int i = 0; i < count; i++){
				log.info("Create Documents from provided JSON Number " + i);
				docs[i] = generateJsonData();
			}
		}
		else{
			final String data = generateData(docType);
			for (int i = 0; i < count; i++) {
				docs[i] = new Document()
				.append("_id", i + startRecord + offset)
				.append("data", data);
			}	
		}

		return docs;

	}

	private String generateData(DocType docType) {
		String data = "";
		if(docType.equals(DocType.lorem)){
			log.info("Create Lorem Data");
			data=generateLoremData();
		}
		else{
			log.info("Create Random Data");
			data = RandomStringUtils.randomAlphabetic(docSize);
		}
		return data;
	}

	private Document generateJsonData(){
		JSONParser parser = new JSONParser();
		Document dbOject = null;
		try {
			FileReader jsonReader = new FileReader(jsonPath);
			String jsonData = parser.parse(jsonReader).toString();
			dbOject = Document.parse(jsonData);
		} catch (Exception e){
			log.error("An Error occured while parsing the JSON data", e);
			System.exit(0);
		}
		return dbOject;

	}

	private String generateLoremData() {
		String sentence = new LoremIpsum((long)Math.random()).getWords(docSize);
		return sentence.substring(0,docSize);
	}
}
