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

import com.mongodb.*;
import com.mongodb.client.*;
import org.apache.commons.lang.RandomStringUtils;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.Random;
import java.util.Locale;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

/**
 * RunThread for MongoDB benchmarking.
 * This class runs the logic for stressing the DBs.
 */
public class RunThread implements Runnable {

    /**
     * DB Statistics holder.
     * This class holds the statistics for a particular DB.
     */
    private class DbStats {
        public long DbIdx = 0;
        public String uri;
        public List<String> host;
        public String username;
        public String password;
        public String replica;
        public boolean sslEnabled;

        public long numReads = 0;
        public long numUpdates = 0;
        public long numQueries = 0;
        public long timeouts = 0;
        public long maxReadLatency = 0;
        public long minReadLatency = Long.MAX_VALUE;
        public long maxUpdateLatency = 0;
        public long minUpdateLatency = Long.MAX_VALUE;
        public long maxQueryLatency = 0;
        public long minQueryLatency = Long.MAX_VALUE;
        public float accReadLatencies = 0;
        public float accUpdateLatencies = 0;
        public float accQueryLatencies = 0;

        public DbStats(int DbIdx, String uri) {
            this.DbIdx = DbIdx;
            this.uri = uri;
            MongoURI.parseURI(uri);
            this.host = MongoURI.host;
            this.username = MongoURI.username;
            this.password = MongoURI.password;
            this.replica = MongoURI.replica;
            this.sslEnabled = MongoURI.isSSLEnabled;
        }

        public void resetStat() {
            numReads = 0;
            numUpdates = 0;
            numQueries = 0;
            timeouts = 0;
            maxReadLatency = 0;
            minReadLatency = Long.MAX_VALUE;
            maxUpdateLatency = 0;
            minUpdateLatency = Long.MAX_VALUE;
            maxQueryLatency = 0;
            minQueryLatency = Long.MAX_VALUE;
            accReadLatencies = 0;
            accUpdateLatencies = 0;
            accQueryLatencies = 0;
        }

        public void pushNewReadLatency(long readLatency) {
            numReads++;
            if (readLatency < minReadLatency) {
                minReadLatency = readLatency;
            }
            if (readLatency > maxReadLatency) {
                maxReadLatency = readLatency;
            }
            accReadLatencies += readLatency;
        }
        
        public void pushNewUpdateLatency(long updateLatency) {
            numUpdates++;
            if (updateLatency < minUpdateLatency) {
                minUpdateLatency = updateLatency;
            }
            if (updateLatency > maxUpdateLatency) {
                maxUpdateLatency = updateLatency;
            }
            accUpdateLatencies += updateLatency;
        }
 
        public void pushNewQueryLatency(long queryLatency) {
            numQueries++;
            if (queryLatency < minUpdateLatency) {
                minUpdateLatency = queryLatency;
            }
            if (queryLatency > maxUpdateLatency) {
                maxUpdateLatency = queryLatency;
            }
            accQueryLatencies += queryLatency;
        }
       
        public float getRate(long elapsed) {
            return ((float) (numUpdates + numReads + numQueries) * 1000f) / (float) elapsed;
        }

        public double getAvgReadLatencyMs() {
            return accReadLatencies/(numReads * 1e6);
        }

        public double getAvgUpdateLatencyMs() {
            return accUpdateLatencies/(numUpdates * 1e6);
        }

        public double getAvgQueryLatencyMs() {
            return accQueryLatencies/(numQueries * 1e6);
        }

        public String hostList() {
            return host.toString().replace(", ", ",");
        }
    }

    private static final Logger log = LoggerFactory.getLogger(RunThread.class);
    private AtomicBoolean stop = new AtomicBoolean(false);
    private int id = -1;
    private int targetReads = 9;   // Update after this much reads
    private int currentReads = 0;
    private int numDocuments = 0;
    private int numDbs = 0;
    private int numInserts = 0;
    private int numReads = 0;
    private int numQueries = 0;
    private String data = RandomStringUtils.randomAlphabetic(1024);
    private long maxReadLatency = 0;
    private long minReadLatency = Long.MAX_VALUE;
    private long maxUpdateLatency = 0;
    private long minUpdateLatency = Long.MAX_VALUE;
    private long maxQueryLatency = 0;
    private long minQueryLatency = Long.MAX_VALUE;
    private float accReadLatencies = 0;
    private float accUpdateLatencies = 0;
    private float accQueryLatencies = 0;
    private AtomicBoolean initialized = new AtomicBoolean(false);
    private final float targetRate;
    private long startMillis;
    private long elapsed = 0l;
    private FileOutputStream readLatencySink;
    private FileOutputStream insertLatencySink;
    private String lineSeparator = System.getProperty("line.separator");
    private String prefixLatencyFile;
    private int timeoutMs;
    private DbStats[] dbStats;
    private Random rand;
    private List<String> mongoUri;
    private boolean isQuery;        // Check if query mode is on

    public RunThread(int id, List<String> mongoUri, int numDocuments, float targetRate, 
                    String prefixLatencyFile, int timeout, boolean isQuery) {
        this.id = id;
        this.mongoUri = mongoUri;
        this.numDbs = mongoUri.size();
        this.numDocuments = numDocuments;
        this.targetRate = targetRate;
        this.prefixLatencyFile = prefixLatencyFile;
        this.timeoutMs = timeout * 1000;
        this.isQuery = isQuery;
        
        rand = new Random();
        
        dbStats = new DbStats[numDbs];
    }


    @Override
    public void run() {
        final MongoClient[] clients = new MongoClient[numDbs];
        log.info("Thread {} opening {} connections", id, numDbs);
        for (int i = 0; i < numDbs; i++) {
            String uri = mongoUri.get(i);
            MongoURI.parseURI(uri);
            boolean sslEnabled = MongoURI.isSSLEnabled;

            dbStats[i] = new DbStats(i, uri);

            final MongoClientOptions ops = MongoClientOptions.builder()
                    .maxWaitTime(timeoutMs)
                    .connectTimeout(timeoutMs)
                    .socketTimeout(timeoutMs)
                    .heartbeatConnectTimeout(timeoutMs)
                    .serverSelectionTimeout(timeoutMs)
                    .sslEnabled(sslEnabled)
                    .build();

            log.info("Thread {} connecting to database URI {}", id, uri);

            MongoClientURI cUri = new MongoClientURI(uri, new MongoClientOptions.Builder(ops));
            clients[i] = new MongoClient(cUri);
        }

        if (prefixLatencyFile != null) {
            try {
                readLatencySink = new FileOutputStream(prefixLatencyFile + "_read_" + Thread.currentThread().getId());
                insertLatencySink = new FileOutputStream(prefixLatencyFile + "_insert_" + Thread.currentThread().getId());
            } catch (IOException e) {
                log.error("Unable to open latency file", e);
            }
        }

        // Required for randomizing the accesses to the DBs
        List<Integer> clientList = new ArrayList<Integer>();
        for(int i=0;i<clients.length;i++) {
            clientList.add(i);
        }
        int readIdx = 0;
        int clientIdx = 0;
        initialized.set(true);
        long ratePause = (long) (1000f / targetRate);
        startMillis = System.currentTimeMillis();
        float currentRate = 0;

        int timeouts = 0;

        // do the actual benchmark measurements
        try {
            while (!stop.get()) {
                if (!isQuery) {     // Read/write tests
                    if (targetRate > 0) {
                        if ((float) (numReads + numInserts) * 1000f / (float) (System.currentTimeMillis()
                                                - startMillis) > targetRate) {
                            sleep(ratePause);
                        }
                    }
                    clientIdx = clientList.get(readIdx);
                    if (currentReads < targetReads) {
                        currentReads++;
                        try {
                            readRecord(clientIdx, clients[clientIdx]);
                        } catch (MongoSocketException | MongoTimeoutException e) {
                            timeouts++;
                            dbStats[clientIdx].timeouts++;
                            log.warn("Timeout occured for thread {} while reading from {}:{}. Trying to reconnect client No. {}", 
                                            id,
                                            clients[clientIdx].getAddress().getHost(), 
                                            clients[clientIdx].getAddress().getPort(), clientIdx);
                            final MongoClientOptions ops = clients[clientIdx].getMongoClientOptions();
                            final ServerAddress address = clients[clientIdx].getAddress();
                            clients[clientIdx].close();
                            clients[clientIdx] = new MongoClient(address, ops);
                            log.info("Reconnected to {}:{}", clients[clientIdx].getAddress().getHost(), 
                                            clients[clientIdx].getAddress().getPort());
                        }
                    } else {
                        currentReads = 0;
                        try {
                            updateRecord(clientIdx, clients[clientIdx]);
                        } catch (MongoSocketException | MongoTimeoutException e) {
                            timeouts++;
                            dbStats[clientIdx].timeouts++;
                            log.warn("Timeout occured for thread {} while writing to {}:{}. Trying to reconnect client No. {}", 
                                            id,
                                            clients[clientIdx].getAddress().getHost(), 
                                            clients[clientIdx].getAddress().getPort(), clientIdx);
                            final MongoClientOptions ops = clients[clientIdx].getMongoClientOptions();
                            final ServerAddress address = clients[clientIdx].getAddress();
                            clients[clientIdx].close();
                            clients[clientIdx] = new MongoClient(address, ops);
                            log.info("Reconnected to {}:{}", clients[clientIdx].getAddress().getHost(), 
                                            clients[clientIdx].getAddress().getPort());
                        }
                    }
                    readIdx = readIdx + 1 < clientList.size() ? readIdx + 1 : 0;
                    if(readIdx == 0) {
                        Collections.shuffle(clientList);
                    }
                } else {        // Query mode requrested
                    try {
                        performQuery(clientIdx, clients[clientIdx]);
                    } catch (MongoSocketException | MongoTimeoutException e) {
                        timeouts++;
                        dbStats[clientIdx].timeouts++;
                        log.warn("Timeout occured for thread {} while quering to {}:{}. Trying to reconnect client No. {}", 
                                       id,
                                       clients[clientIdx].getAddress().getHost(), 
                                       clients[clientIdx].getAddress().getPort(), clientIdx);
                        final MongoClientOptions ops = clients[clientIdx].getMongoClientOptions();
                        final ServerAddress address = clients[clientIdx].getAddress();
                        clients[clientIdx].close();
                        clients[clientIdx] = new MongoClient(address, ops);
                        log.info("Reconnected to {}:{}", clients[clientIdx].getAddress().getHost(), 
                                       clients[clientIdx].getAddress().getPort());
                    }
                }
            }
            elapsed = System.currentTimeMillis() - startMillis;
        } catch (IOException e) {
            log.error("Error while running benchmark", e);
        }

        log.info("Thread {} closing {} connections", id, clients.length);
        for (final MongoClient c : clients) {
            c.close();
        }

        try {
            if (insertLatencySink != null) {
                insertLatencySink.close();
            }
            if (readLatencySink != null) {
                readLatencySink.close();
            }
        } catch (IOException e) {
            log.error("Unable to close stream", e);
        }

        log.info("Thread {} finished with {} timeouts", id, timeouts);
    }

    private void sleep(long ratePause) {
        try {
            Thread.sleep(ratePause);
        } catch (InterruptedException e) {
            log.error("Error while sleeping", e);
        }
    }

    public float getRate() {
        return ((float) (numInserts + numReads + numQueries) * 1000f) / (float) elapsed;
    }

    private void updateRecord(int clientIdx, MongoClient client) throws IOException {
        final int randKey = rand.nextInt(numDocuments);
        final Document doc = new Document("_id", randKey); 
        data = RandomStringUtils.randomAlphabetic(1024);
        long start = System.nanoTime();
        client.getDatabase(MongoBench.DB_NAME).getCollection(MongoBench.COLLECTION_NAME)
                .updateOne(doc, new Document("$set", new Document("data", data)));
        long latency = System.nanoTime() - start;
        recordLatency(latency, insertLatencySink);
        if (latency < minUpdateLatency) {
            minUpdateLatency = latency;
        }
        if (latency > maxUpdateLatency) {
            maxUpdateLatency = latency;
        }
        
        accUpdateLatencies += latency;
        numInserts++;
        dbStats[clientIdx].pushNewUpdateLatency(latency);
    }

    private void readRecord(int clientIdx, MongoClient client) throws IOException {
        final int randKey = rand.nextInt(numDocuments);
        final Document doc = new Document("_id", randKey); 
        long start = System.nanoTime();
        final Document fetched = client.getDatabase(MongoBench.DB_NAME)
                .getCollection(MongoBench.COLLECTION_NAME).find(doc).first();
        long latency = System.nanoTime() - start;
        recordLatency(latency, readLatencySink);
        if (latency < minReadLatency) {
            minReadLatency = latency;
        }
        if (latency > maxReadLatency) {
            maxReadLatency = latency;
        }
        
        accReadLatencies += latency;
        if (fetched == null) {
            log.warn("Thread {} client {} Unable to read document with id {}", 
                            id, clientIdx,
                            doc.get("_id"));
        }
        numReads++;
        dbStats[clientIdx].pushNewReadLatency(latency);
    }

    private void performQuery(int clientIdx, MongoClient client) throws IOException {
        BasicDBObject regexQuery = new BasicDBObject();
        regexQuery.put("data", new BasicDBObject("$regex", "^lr.*").append("$options", "i"));
        long start = System.nanoTime();
        FindIterable<Document> cursor = client.getDatabase(MongoBench.DB_NAME)
                                .getCollection(MongoBench.COLLECTION_NAME)
                                .find(regexQuery); 
        long latency = System.nanoTime() - start;
        if (latency < minQueryLatency) {
            minQueryLatency = latency;
        }
        if (latency > maxQueryLatency) {
            maxQueryLatency = latency;
        }
        
        accQueryLatencies += latency;
        numQueries++;

        if (cursor != null) {
            dbStats[clientIdx].pushNewQueryLatency(latency);
        }
    }

    private void recordLatency(final long latency, final FileOutputStream sink) throws IOException {
        if (sink != null) {
            sink.write(String.valueOf(latency).getBytes());
            sink.write(lineSeparator.getBytes());
            sink.flush();
        }
    }

    public void stop() {
        stop.set(true);
    }

    public int getNumInserts() {
        return numInserts;
    }

    public int getNumReads() {
        return numReads;
    }

    public int getNumQueries() {
        return numQueries;
    }

    public long getMaxReadlatency() {
        return maxReadLatency;
    }

    public long getMaxWriteLatency() {
        return maxUpdateLatency;
    }
 
    public long getMaxQueryLatency() {
        return maxQueryLatency;
    }

    public long getMinReadLatency() {
        return minReadLatency;
    }

    public long getMinWriteLatency() {
        return minUpdateLatency;
    }

    public long getMinQueryLatency() {
        return minQueryLatency;
    }

    public float getAccReadLatencies() {
        return accReadLatencies;
    }

    public float getAccWriteLatencies() {
        return accUpdateLatencies;
    }

    public float getAccQueryLatencies() {
        return accQueryLatencies;
    }


    public boolean isInitialized() {
        return initialized.get();
    }

    public synchronized void resetData() {
        numInserts = 0;
        numReads = 0;
        numQueries = 0;
        maxReadLatency = 0;
        minReadLatency = Long.MAX_VALUE;
        maxUpdateLatency = 0;
        minUpdateLatency = Long.MAX_VALUE;
        maxQueryLatency = 0;
        minQueryLatency = Long.MAX_VALUE;
        accReadLatencies = 0;
        accUpdateLatencies = 0;
        startMillis = System.currentTimeMillis();

        for(int i=0;i<numDbs;i++) {
            dbStats[i].resetStat();
        }
    }

    public void writeDbStats(PrintWriter pw) {
        for(int i=0;i<numDbs;i++) {
            pw.format(Locale.US, "%d %d %.2f %d %d %d %.2f %d %d %d %.2f %d %d %d %.2f %d %s",
                            id, elapsed/1000, dbStats[i].getRate(elapsed),
                            dbStats[i].numReads, dbStats[i].minReadLatency,
                            dbStats[i].maxReadLatency, dbStats[i].getAvgReadLatencyMs(),
                            dbStats[i].numUpdates, dbStats[i].minUpdateLatency,
                            dbStats[i].maxUpdateLatency, dbStats[i].getAvgUpdateLatencyMs(),
                            dbStats[i].numQueries, dbStats[i].minQueryLatency,
                            dbStats[i].maxQueryLatency, dbStats[i].getAvgQueryLatencyMs(),
                            dbStats[i].timeouts, dbStats[i].uri);                          
            pw.println(); 
        }

    }
}
