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

import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.RandomStringUtils;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;

public class LoadThread implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(LoadThread.class);

    private final int id;
    private final List<String> mongoUri;     // A list with MongoDB URI enteries
    private final int numDocuments;
    private final int numDocsToInsert;
    private final int docSize;
    private final int maxBatchSize = 1000;
    private final int timeoutMs;
    private final Map<String, Integer> failed = new HashMap<>();
    private final static DecimalFormat decimalFormat = new DecimalFormat("0.0000");
    private final int startRecord;          // Starting insert record number
    private final int endRecord;            // Ending insert record number

    public LoadThread(int id, List<String> mongoUri, int numDocuments, int docSize, int timeout, 
                int startRecord, int endRecord) {
        this.id = id;
        this.mongoUri = mongoUri;
        this.numDocuments = numDocuments;
        this.docSize = docSize;
        this.timeoutMs = timeout * 1000;
        this.startRecord = startRecord;
        this.endRecord = endRecord;
        this.numDocsToInsert = endRecord - startRecord + 1;
    }

    @Override
    public void run() {
        log.info("Thread {} loading {} docs into {} instances", id, numDocsToInsert, mongoUri.size());
        for (int i = 0; i < mongoUri.size(); i++) {
            String uri = mongoUri.get(i);
            log.info("Thread {} connnecting to database URI {}", id, uri);
        
            MongoURI.parseURI(uri); 
            List<String> host = MongoURI.host;
            String username = MongoURI.username;
            String password = MongoURI.password;
            String replica = MongoURI.replica;
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

            for (final String name : client.listDatabaseNames()) {
                if (name.equalsIgnoreCase(MongoBench.DB_NAME)) {
                    log.warn("Database {} exists and will be purged before inserting", MongoBench.DB_NAME);
                    client.dropDatabase(name);
                    break;
                }
            }
            long startLoad = System.currentTimeMillis();
            while (count < numDocsToInsert) {
                currentBatchSize = numDocsToInsert - count > maxBatchSize ? maxBatchSize : numDocsToInsert - count;
                final Document[] docs = createDocuments(currentBatchSize, count);
                final MongoCollection<Document> collection = client.getDatabase(MongoBench.DB_NAME).getCollection(MongoBench.COLLECTION_NAME);
                try {
                    collection.insertMany(Arrays.asList(docs));
                } catch (Exception e) {
                    log.error("Error while inserting {} documents at {}", currentBatchSize, host);
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

    private Document[] createDocuments(int count, int offset) {
        final String data = RandomStringUtils.randomAlphabetic(docSize);
        final Document[] docs = new Document[count];
        for (int i = 0; i < count; i++) {
            docs[i] = new Document()
                    .append("_id", i + startRecord + offset)
                    .append("data", data);
        }
        return docs;
    }
}
