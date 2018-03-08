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
import org.apache.commons.lang.RandomStringUtils;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.Random;

public class RunThread implements Runnable {

	/**
	 * DBStats.
	 * This class holds the statistics for a particular DB.
	 */
	private class DbStats {
		 public long DbIdx = 0;
		 public String host;
		 public int port = 0;
		 public long numReads = 0;
		 public long numUpdates = 0;
		 public long timeouts = 0;
		 public long maxReadLatency = 0;
		 public long minReadLatency = Long.MAX_VALUE;
		 public long maxWriteLatency = 0;
		 public long minWriteLatency = Long.MAX_VALUE;
		 public float accReadLatencies = 0;
		 public float accWriteLatencies = 0;

		public DbStats(int DbIdx, String host, int port) {
			this.DbIdx = DbIdx;
			this.host = host;
			this.port = port;
		}

		public resetStat() {
			numReads = 0;
        	numUpdates = 0;
			timeouts = 0;
        	maxReadLatency = 0;
        	minReadLatency = Long.MAX_VALUE;
        	maxWriteLatency = 0;
        	minWriteLatency = Long.MAX_VALUE;
			accReadLatencies = 0;
			accWriteLatencies = 0;
		}
	}

    private static final Logger log = LoggerFactory.getLogger(RunThread.class);
    private AtomicBoolean stop = new AtomicBoolean(false);
    private int id = -1;
	private float targetRatio = 0.9f;
    private float currentRatio = 0f;
	private int numDocuments = 0;
	private int numDbs = 0;
    private int numInserts = 0;
    private int numReads = 0;
    private final List<String> ipPorts;	// @todo This might not be required.
    private String data = RandomStringUtils.randomAlphabetic(1024);
    private long maxReadLatency = 0;
    private long minReadLatency = Long.MAX_VALUE;
    private long maxWriteLatency = 0;
    private long minWriteLatency = Long.MAX_VALUE;
    private float accReadLatencies = 0;
    private float accWriteLatencies = 0;
    private AtomicBoolean initialized = new AtomicBoolean(false);
    private final float targetRate;
    private long startMillis;
    private long elapsed = 0l;
    private FileOutputStream readLatencySink;
    private FileOutputStream insertLatencySink;
    private String lineSeparator = System.getProperty("line.separator");
    private String prefixLatencyFile;
    private int timeoutMs;
    private final boolean sslEnabled;
	private DbStats[] dbStats;
	private Random rand;

    public RunThread(int id, List<String> ipPorts, int numDocuments, float targetRate, 
					String prefixLatencyFile, int timeout, boolean sslEnabled) {
        this.id = id;
        this.ipPorts = ipPorts;
		this.numDbs = ipPorts.size();
		this.numDocuments = numDocuments;
        this.targetRate = targetRate;
        this.prefixLatencyFile = prefixLatencyFile;
        this.timeoutMs = timeout * 1000;
        this.sslEnabled = sslEnabled;
		
        rand = new Random();
		
		dbStats = new DbStats[numDbs];
		for(int i = 0;i < numDbs; i++) {
            String[] parts = ipPorts.get(i).split(":");
			dbStats[i] = new DbStats(i, parts[0], Integer.parseInt(parts[1]));
		}
	}


    @Override
    public void run() {
        int portsLen = ipPorts.size();
        final MongoClient[] clients = new MongoClient[portsLen];
        log.info("Opening {} connections", portsLen);
        for (int i = 0; i < portsLen; i++) {
            final MongoClientOptions ops = MongoClientOptions.builder()
                    .maxWaitTime(timeoutMs)
                    .connectTimeout(timeoutMs)
                    .socketTimeout(timeoutMs)
                    .heartbeatConnectTimeout(timeoutMs)
                    .serverSelectionTimeout(timeoutMs)
                    .sslEnabled(sslEnabled)
                    .build();
			String host = dbStats[i].host;
			int port = dbStats[i].port;
			clients[i] = new MongoClient(new ServerAddress(host, port), ops);
        }

        if (prefixLatencyFile != null) {
            try {
                readLatencySink = new FileOutputStream(prefixLatencyFile + "_read_" + Thread.currentThread().getId());
                insertLatencySink = new FileOutputStream(prefixLatencyFile + "_insert_" + Thread.currentThread().getId());
            } catch (IOException e) {
                log.error("Unable to open latency file", e);
            }
        }

        int clientIdx = 0;
        initialized.set(true);
        long ratePause = (long) (1000f / targetRate);
        startMillis = System.currentTimeMillis();
        float currentRate = 0;

        int timeouts = 0;

        // do the actual benchmark measurements
        try {
            while (!stop.get()) {
                currentRatio = (float) numReads / (float) (numInserts + numReads);
                if (targetRate > 0) {
                    if ((float) (numReads + numInserts) * 1000f / (float) (System.currentTimeMillis()
										   	- startMillis) > targetRate) {
                        sleep(ratePause);
                    }
                }
                clientIdx = clientIdx + 1 < clients.length ? clientIdx + 1 : 0;
                if (currentRatio < targetRatio) {
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
                elapsed = System.currentTimeMillis() - startMillis;
            }
        } catch (IOException e) {
            log.error("Error while running benchmark", e);
        }

        log.info("Closing {} connections", clients.length);
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

        log.info("Thread finished with {} timeouts", timeouts);
    }

    private void sleep(long ratePause) {
        try {
            Thread.sleep(ratePause);
        } catch (InterruptedException e) {
            log.error("Error while sleeping", e);
        }
    }

    public float getRate() {
        return ((float) (numInserts + numReads) * 1000f) / (float) elapsed;
    }

    private void updateRecord(int clientIdx, MongoClient client) throws IOException {
        final int randKey = rand.nextInt(numDocuments);
        final Document doc = new Document("_id", randKey); 
        long start = System.nanoTime();
        client.getDatabase(MongoBench.DB_NAME).getCollection(MongoBench.COLLECTION_NAME)
				.updateOne(doc, new Document("$set", new Document("data", data)));
        long latency = System.nanoTime() - start;
        recordLatency(latency, insertLatencySink);
        if (latency < minWriteLatency) {
            minWriteLatency = latency;
        }
		if (latency < dbStats[clientIdx].minWriteLatency) {
            dbStats[clientIdx].minWriteLatency = latency;
        }
        if (latency > maxWriteLatency) {
            maxWriteLatency = latency;
        }
		if (latency > dbStats[clientIdx].maxWriteLatency) {
            dbStats[clientIdx].maxWriteLatency = latency;
        }
        accWriteLatencies += latency;
		dbStats[clientIdx].accWriteLatencies += latency;
        numInserts++;
		dbStats[clientIdx].numUpdates++;
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
		if (latency < dbStats[clientIdx].minReadLatency) {
            dbStats[clientIdx].minReadLatency = latency;
		}
		if (latency > dbStats[clientIdx].maxReadLatency) {
            dbStats[clientIdx].maxReadLatency = latency;
        }
        accReadLatencies += latency;
		dbStats[clientIdx].accReadLatencies += latency;
        if (fetched == null) {
            log.warn("Thread {} client {} Unable to read document with id {}", 
							id, clientIdx,
							doc.get("_id"));
        }
        numReads++;
		dbStats[clientIdx].numReads++;
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

    public long getMaxReadlatency() {
        return maxReadLatency;
    }

    public long getMaxWriteLatency() {
        return maxWriteLatency;
    }

    public long getMinReadLatency() {
        return minReadLatency;
    }

    public long getMinWriteLatency() {
        return minWriteLatency;
    }

    public float getAccReadLatencies() {
        return accReadLatencies;
    }

    public float getAccWriteLatencies() {
        return accWriteLatencies;
    }


    public boolean isInitialized() {
        return initialized.get();
    }

    public synchronized void resetData() {
        numInserts = 0;
        numReads = 0;
        maxReadLatency = 0;
        minReadLatency = Long.MAX_VALUE;
        maxWriteLatency = 0;
        minWriteLatency = Long.MAX_VALUE;
        accReadLatencies = 0;
        accWriteLatencies = 0;
        startMillis = System.currentTimeMillis();

   		for(int i=0;i<numDbs;i++) {
			dbStats[i].resetStat();
		}
	}
}
