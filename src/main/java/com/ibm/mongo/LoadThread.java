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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientURI;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;

public class LoadThread implements Runnable {

	private static final Logger log = LoggerFactory.getLogger(LoadThread.class);

	private final int id;
	private final List<String> mongoUri;     // A list with MongoDB URI enteries
	private final int numDocsToInsert;
	private final int maxBatchSize = 100;
	private final int timeoutMs;
	private final Map<String, Integer> failed = new HashMap<>();
	private final DocType docType;
	private boolean skipDrop;

	private int startRecord;

	private int docSize;

	public enum DocType {
		random, lorem 
	}

	public LoadThread(int id, List<String> mongoUri, int docSize, int timeout, 
			int startRecord, int endRecord, DocType doctype, boolean skipDrop) {
		this.id = id;
		this.mongoUri = mongoUri;
		this.timeoutMs = timeout * 1000;
		this.numDocsToInsert = endRecord - startRecord + 1;
		this.docType = doctype;
		this.skipDrop = skipDrop;
		this.startRecord = startRecord;
		this.docSize = docSize;
	}

	@Override
	public void run() {
		log.info("Thread {} loading {} docs into {} instances with Ids {} to {}", id, numDocsToInsert, mongoUri.size(), startRecord, startRecord + numDocsToInsert-1);
		for (int i = 0; i < mongoUri.size(); i++) {
			String uri = mongoUri.get(i);
			log.info("Thread {} connnecting to database URI {}", id, uri);

			MongoURI.parseURI(uri); 
			List<String> host = MongoURI.host;
			boolean sslEnabled = MongoURI.isSSLEnabled;

			int count = 0;
			int currentBatchSize = 0;

			MongoClient client = createMongoClient(uri, sslEnabled);

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
				try {
					currentBatchSize = processBatch(count, client);
					this.startRecord = this.startRecord + currentBatchSize;
					count += currentBatchSize;
				} catch (Exception e) {
					while(true){
						try {
							log.error("Error while inserting {} documents at {}", currentBatchSize, host, e);
							log.warn("Thread {} no connection to {}. Reconnecting...", id, host);
							client.close();
							client = createMongoClient(uri, sslEnabled);
							client.getDatabase(MongoBench.DB_NAME).getCollection(MongoBench.COLLECTION_NAME);
							break;
						} catch (Exception e2) {
							continue;	
						}
					}
				}
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

	private MongoClient createMongoClient(String uri, boolean sslEnabled) {
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
		return  new MongoClient(cUri);
	}

	private int processBatch(int batchSize, MongoClient client) {
		int currentBatchSize = numDocsToInsert - batchSize > maxBatchSize ? maxBatchSize : numDocsToInsert - batchSize;
		final Document[] docs = DataGeneration.createDocuments(currentBatchSize, docType, startRecord, docSize);
		final MongoCollection<Document> mongoCollection = client.getDatabase(MongoBench.DB_NAME).getCollection(MongoBench.COLLECTION_NAME);
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
		return currentBatchSize;
	}

	private boolean CheckIfDocumentExists(Object iDToFind , MongoCollection<Document> mongoCollection) {
		FindIterable<Document> find = mongoCollection.find(new Document("_id",iDToFind));
		return find.first() != null;
	}

	
}
