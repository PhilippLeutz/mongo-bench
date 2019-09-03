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

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URISyntaxException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibm.mongo.LoadThread.DocType;
import com.ibm.mongo.indicies.IndicieGenerator;
import com.ibm.mongo.indicies.EqualDistribution;
import com.ibm.mongo.indicies.IndiciesHelperFactory;
import com.ibm.mongo.indicies.IndiciesHelperFactory.HelperType;

public class MongoBench {

	private static final Logger LOG = LoggerFactory.getLogger(MongoBench.class);

	public final static String DB_NAME = "mongo-bench";

	public final static String COLLECTION_NAME = "mongo-bench-documents";

	private final static DecimalFormat DECIMAL_FORMAT = new DecimalFormat("0.0000");

	private enum Phase {
		RUN, LOAD
	}

	public static void main(String[] args) {
		final Options ops = new Options();
		ops.addOption("p", "port", true, "The ports to connect to");
		ops.addOption("t", "target ", true, "The target single host to connect to");
		ops.addOption("l", "phase", true, "The phase to execute [run|load]");
		ops.addOption("d", "duration", true, "Run the bench for this many seconds");
		ops.addOption("n", "num-thread", true, "The number of threads to run");
		ops.addOption("r", "reporting-interval", true, "The interval in seconds for reporting progress");
		ops.addOption("c", "num-documents", true, "The number of documents to create during the load phase");
		ops.addOption("s", "document-size", true, "The size of the created documents");
		ops.addOption("w", "warmup-time", true, "The number of seconds to wait before actually collecting result data");
		ops.addOption("j", "target-rate", true, "Send request at the given rate. Accepts decimal numbers");
		ops.addOption("a", "record-latencies", true, "Set the file prefix to which to write latencies to of all the DBs");
		ops.addOption("o", "timeout", true, "Set the timeouts in seconds for networking operations");
		ops.addOption("u", "ssl", false, "Use SSL for MongoDB connections");
		ops.addOption("e", "user", true, "Username for authentication");
		ops.addOption("k", "password", true, "Password for authentication");
		ops.addOption("i", "replica-set", true, "Name of the replica set to connect");
		ops.addOption("f", "connect-file", true, "Use a connection file with each line containing MongoDB URI"); 
		ops.addOption("b", "offset", true, "number to offset when creating slices for Ids of the documents inserted");
		ops.addOption("q", "query", false, "Search for data starting with \"lr\" case insentive rather than read/write"); 
		ops.addOption("W", "write-rate", true, "Write rate in percent for run-mode. Has to be between 0 and 100. Default: 10");
		ops.addOption("D", "document-type", true, "Document Type, either random (default, lorem (text-like data), or json (reads file from json-path)");
		ops.addOption("h", "help", false, "Show this help dialog");
		ops.addOption("x", "skip-drop", false, "Skip database cleaning, when it already exists");
		ops.addOption("B", true, "Path to the indices of the database beeing queried");
		ops.addOption("T", true, IndiciesHelperFactory.printHelp());
		
		final CommandLineParser parser = new DefaultParser();
		final Phase phase;
		final int[] ports;
		int duration;
		int numThreads;
		int reportingInterval;
		int documentSize;
		int numDocuments;
		int warmup;
		float rateLimit;
		String latencyFilePrefix;
		int timeouts;
		boolean sslEnabled;
		boolean isQuery = false;
		boolean skipDrop = false;
		final String[] mongoUri;
		LoadThread.DocType docType;
		int offesetForSlices;
		int writeRate = 10;
		IndicieGenerator indiciesHelper;

		try {
			final CommandLine cli = parser.parse(ops, args);
			if (cli.hasOption('h')) {
				showHelp(ops);
				return;
			}
			
			if (cli.hasOption('c')) {   // Number of documents/records
				numDocuments = Integer.parseInt(cli.getOptionValue('c'));
			} else {
				numDocuments = 1000;
			}
			
			if (cli.hasOption('l')) {
				if (cli.getOptionValue('l').equalsIgnoreCase("load")) {
					phase = Phase.LOAD;

				} else if (cli.getOptionValue('l').equalsIgnoreCase("run")) {
					phase = Phase.RUN;
				} else {
					throw new ParseException("Invalid phase " + cli.getOptionValue('l'));
				}
			} else {
				throw new ParseException("No phase given. Try \"--help/-h\"");
			}

			String tmpIP = "";
			String tmpUser;
			String tmpPassword;
			String tmpReplica;
			List<Integer> tmpPorts = new ArrayList<Integer>();
			List<String> tmpMongoUri = new ArrayList<String>();


			if (cli.hasOption('p')) {   // Ports
				final String portVal = cli.getOptionValue('p');
				for (final String range : portVal.split(",")) {
					int dashIdx = range.indexOf('-');
					if (dashIdx == -1) {
						tmpPorts.add(Integer.parseInt(range));
					} else {
						int startPort = Integer.parseInt(range.substring(0, dashIdx));
						int endPort = Integer.parseInt(range.substring(dashIdx + 1));
						if (endPort < startPort) {
							throw new ParseException("Port range is invalid. End port must be larger than start port");
						}
						for (int i = 0; i <= endPort - startPort; i++) {
							tmpPorts.add(startPort + i);
						}
					}
				}
			}

			if(cli.hasOption("T")){
				HelperType helperType = IndiciesHelperFactory.HelperType.valueOf(cli.getOptionValue("T"));
				if(cli.hasOption("B")){
					indiciesHelper = IndiciesHelperFactory.createIndiciesWithIndexLimits(cli.getOptionValue("B"), helperType);
				} else{
					indiciesHelper = IndiciesHelperFactory.createIndices(helperType, numDocuments);
				}
				
			} else{
				LOG.info("No indicies type specified, using Equal distribution");
				indiciesHelper = new EqualDistribution(numDocuments);
			}
			
			if (cli.hasOption('u')) { // SSL
				sslEnabled = true;
			} else {
				sslEnabled = false;
			}

			if (cli.hasOption('W')){
				writeRate = Integer.parseInt(cli.getOptionValue('W'));
			}

			if(cli.hasOption('b')){
				offesetForSlices = Integer.parseInt(cli.getOptionValue('b'));
			}
			else{
				offesetForSlices = 0;
			}

			if(cli.hasOption('f')) {    // Connect File
				if (cli.hasOption('t')) {
					throw new ParseException("Cannot use -t and -f together");
				}
				if (cli.hasOption('p')) {
					throw new ParseException("Cannot use -p and -f together");
				}
				if (cli.hasOption('e')) {
					throw new ParseException("Cannot use -e and -f together");
				}
				if (cli.hasOption('k')) {
					throw new ParseException("Cannot use -k and -f together");
				}
				if (cli.hasOption('i')) {
					throw new ParseException("Cannot use -i and -f together");
				}

				final String fileName = cli.getOptionValue('f');
				try(BufferedReader b = new BufferedReader(new FileReader(fileName))) {
					for(String l; (l = b.readLine()) != null; ) {
						tmpMongoUri.add(l);
					}
				}   
				catch (Exception e) {
					System.err.println(e.getMessage());
				}

			} else {
				if (cli.hasOption('t')) {
					tmpIP = cli.getOptionValue('t');
				} else {
					LOG.error("Must provide \"t\" option");
				}
				if (cli.hasOption('e')) {
					tmpUser = cli.getOptionValue('e');
				} else {
					tmpUser = "";
				}
				if (cli.hasOption('k')) {
					tmpPassword = cli.getOptionValue('k');
				} else {
					tmpPassword = "";
				}
				if (cli.hasOption('i')) {
					tmpReplica = cli.getOptionValue('i');
				} else {
					tmpReplica = "";
				}

				ports = new int[tmpPorts.size()];

				for(int i=0;i<tmpPorts.size();i++) {
					ports[i] = tmpPorts.get(i);
					List<String> tmpHost = new 
							ArrayList<String>(Arrays.asList(tmpIP + ":" + Integer.toString(tmpPorts.get(i))));
					String uri = MongoURI.createURI(tmpHost, tmpUser, tmpPassword,
							tmpReplica, sslEnabled); 
					tmpMongoUri.add(uri);
				}
			}

			mongoUri = new String[tmpMongoUri.size()];
			for (int i=0;i<tmpMongoUri.size(); i++) {
				mongoUri[i] = tmpMongoUri.get(i);
			}

			if (cli.hasOption('d')) {   // Duration
				duration = Integer.parseInt(cli.getOptionValue('d'));
			} else {
				duration = 60;
			}
			if (cli.hasOption('n')) {   // Threads
				numThreads = Integer.parseInt(cli.getOptionValue('n'));
			} else {
				numThreads = 1;
			}
			if (cli.hasOption('r')) {   // Report Interval
				reportingInterval = Integer.parseInt(cli.getOptionValue('r'));
			} else {
				reportingInterval = 60;
			}
			if (cli.hasOption('s')) {   // Size of documents
				documentSize = Integer.parseInt(cli.getOptionValue('s'));
			} else {
				documentSize = 1024;
			}
			if (cli.hasOption('w')) {   // Warmup time
				warmup = Integer.parseInt(cli.getOptionValue('w'));
			} else {
				warmup = 0;
			}
			if (cli.hasOption('j')) {   // Rate limit per thread
				rateLimit = Float.parseFloat(cli.getOptionValue('j'));
			} else {
				rateLimit = 0f;
			}
			if (cli.hasOption('a')) {   // Latency file
				latencyFilePrefix = cli.getOptionValue('a');
			} else {
				latencyFilePrefix = null;
			}
			if (cli.hasOption('o')) {   // Time value
				timeouts = Integer.parseInt(cli.getOptionValue('o'));
			} else {
				timeouts = 30;
			}
			if (cli.hasOption('q')) {   // Query
				isQuery = true;
			}

			LOG.info("Running phase {}", phase.name());

			if (cli.hasOption('D')){
				docType = DocType.valueOf(cli.getOptionValue('D'));
				if (docType == null){
					LOG.error("Unable to parse doctype value");
					System.exit(0);
				}
			} else {
				docType = DocType.random;
			}
			LOG.info("Using Data-type {}", docType);

			if (cli.hasOption('x')) { // skip Drop DB
				skipDrop= true;
			} else {
				skipDrop = false;
			}

			
			final MongoBench bench = new MongoBench();
			if (phase == Phase.LOAD) {
				bench.doLoadPhase(mongoUri, numThreads, numDocuments, documentSize, timeouts, docType, skipDrop, offesetForSlices);
			} else {
				bench.doRunPhase(mongoUri, warmup, duration, numThreads, reportingInterval, 
						rateLimit, latencyFilePrefix, timeouts, isQuery, writeRate, documentSize, docType, indiciesHelper);
			}

		} catch (ParseException | IOException e) {
			LOG.error("Unable to parse", e);
		}
	}

	private void doRunPhase(String[] mongoUri, int warmup, int duration, int numThreads, 
			int reportingInterval, float targetRate, String latencyFilePrefix, int timeouts, boolean isQuery, int writeRate, int docSize, DocType docType, IndicieGenerator indiciesHelper) {
		LOG.info("Starting {} threads for {} instances", numThreads, mongoUri.length);
		final Map<RunThread, Thread> threads = new HashMap<RunThread, Thread>(numThreads);

		List<List<String>> slices;
		slices = createSlices(mongoUri, numThreads);

		for (int i = 0; i < numThreads; i++) {
			RunThread t = new RunThread(i, slices.get(i), targetRate / (float) numThreads, 
					latencyFilePrefix, timeouts, isQuery, writeRate, docSize, docType, indiciesHelper);
			threads.put(t, new Thread(t));
		}
		for (final Thread t : threads.values()) {
			t.start();
		}

		for (final RunThread r : threads.keySet()) {
			while (!r.isInitialized()) {
				Thread.yield();
			}
		}
		LOG.info("Client threads have been initialized");

		// run the warmup phase id a warmup greater than 0 has been passed by the user
		warmup(warmup);

		for (RunThread r : threads.keySet()) {
			r.resetData();
		}

		long start = System.currentTimeMillis();
		long lastInterval = start;
		long currentMillis = System.currentTimeMillis();
		long interval;
		while ((interval = currentMillis - start) < 1000 * duration) {
			if (currentMillis - lastInterval > reportingInterval * 1000) {
				collectAndReportLatencies(threads.keySet(), interval);
				lastInterval = currentMillis;
			}
			try {
				Thread.sleep(300);
			} catch (InterruptedException e) {
				LOG.error("Unable to sleep", e);
			}
			currentMillis = System.currentTimeMillis();
		}
		for (RunThread r : threads.keySet()) {
			r.stop();
		}
		long elapsed = System.currentTimeMillis() - start;
		for (Thread t : threads.values()) {
			try {
				t.join();
			} catch (InterruptedException e) {
				LOG.error("Unable to join thread", e);
			}
		}

		float avgRatePerThread = 0f;
		long numReads = 0;
		long numInserts = 0;
		long numQueries = 0;
		for (final RunThread r : threads.keySet()) {
			avgRatePerThread += r.getRate();
			numInserts += r.getNumInserts();
			numReads += r.getNumReads();
			numQueries += r.getNumQueries();
		}
		float rate = (float) (numReads + numInserts + numQueries) * 1000f / (float) elapsed;
		avgRatePerThread = avgRatePerThread / (float) numThreads;
		LOG.info("Read {}, updated {} and queried {} documents in {} secs", 
				numReads, numInserts, numQueries, DECIMAL_FORMAT.format((float) elapsed / 1000f));
		LOG.info("Overall transaction rate: {} transactions/second", DECIMAL_FORMAT.format(rate));
		LOG.info("Average transaction rate per thread: {} transactions/second", DECIMAL_FORMAT.format(avgRatePerThread));
		LOG.info("Average transaction rate per instance: {} transactions/second", DECIMAL_FORMAT.format(rate / (float) mongoUri.length));
		collectAndReportLatencies(threads.keySet(), elapsed);

		// Write the per DB stats to a file
		try {
			PrintWriter pw = new PrintWriter("/tmp/per_db_stats.txt", "UTF-8");
			pw.println("Thread time[s] tps numRds minRdLat[ns] maxRdLat[ns] AvgRdLat[ms] "
					+ "numUpdts minUpdtLat[ns] maxUpdtLat[ns] AvgUpdtLat[ms] "
					+ "numQueries minQLat[ns] maxQLat[ns] AvgQLat[ms] timeouts uri");
			for (final RunThread r : threads.keySet()) {
				r.writeDbStats(pw);
			}
			pw.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

		LOG.info("Find the DB stats in /tmp/per_db_stats.txt file");
	}



	private void warmup(int warmupInSeconds) {
		if (warmupInSeconds > 0) {
			long startWarmup = System.currentTimeMillis();
			LOG.info("Warm up for {} seconds", warmupInSeconds);
			while (System.currentTimeMillis() - startWarmup < warmupInSeconds * 1000) {
				try {
					Thread.sleep(500);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			LOG.info("Warmup finished.");
		}
	}

	private void collectAndReportLatencies(Set<RunThread> threads, long duration) {
		int numInserts = 0, numReads = 0, numQueries = 0;
		float minReadLatency = Float.MAX_VALUE, maxReadLatency = 0f;
		float minWriteLatency = Float.MAX_VALUE, maxWriteLatency = 0f;
		float minQueryLatency = Float.MAX_VALUE, maxQueryLatency = 0f;
		float avgReadLatency = 0f, avgWriteLatency = 0f, avgQueryLatency = 0f;
		float tps;
		for (final RunThread r : threads) {
			numReads += r.getNumReads();
			numInserts += r.getNumInserts();
			numQueries += r.getNumQueries();
			if (r.getMaxReadlatency() > maxReadLatency) {
				maxReadLatency = r.getMaxReadlatency();
			}
			if (r.getMaxWriteLatency() > maxWriteLatency) {
				maxWriteLatency = r.getMaxWriteLatency();
			}
			if (r.getMaxQueryLatency() > maxQueryLatency) {
				maxQueryLatency = r.getMaxQueryLatency();
			}
			if (r.getMinReadLatency() < minReadLatency) {
				minReadLatency = r.getMinReadLatency();
			}
			if (r.getMinWriteLatency() < minWriteLatency) {
				minWriteLatency = r.getMinWriteLatency();
			}
			if (r.getMinQueryLatency() < minQueryLatency) {
				minQueryLatency = r.getMinQueryLatency();
			}

			avgReadLatency += r.getAccReadLatencies();
			avgWriteLatency += r.getAccWriteLatencies();
			avgQueryLatency += r.getAccQueryLatencies();
		}
		avgReadLatency = avgReadLatency / numReads;
		avgWriteLatency = avgWriteLatency / numInserts;
		avgQueryLatency = avgQueryLatency / numQueries;
		tps = (numInserts + numReads + numQueries) * 1000f / (duration);
		LOG.info("{} inserts, {} reads, {} queries in {} s, {} requests/sec", numInserts, numReads, numQueries, 
				DECIMAL_FORMAT.format(duration / 1000f), DECIMAL_FORMAT.format(tps));
		LOG.info("Read latency Min/Max/Avg [ms]: {}/{}/{}", DECIMAL_FORMAT.format(minReadLatency / 1000000f),
				DECIMAL_FORMAT.format(maxReadLatency / 1000000f), DECIMAL_FORMAT.format(avgReadLatency / 1000000f));
		LOG.info("Write latency Min/Max/Avg [ms]: {}/{}/{}", DECIMAL_FORMAT.format(minWriteLatency / 1000000f),
				DECIMAL_FORMAT.format(maxWriteLatency / 1000000f), DECIMAL_FORMAT.format(avgWriteLatency / 1000000f));
		LOG.info("Query latency Min/Max/Avg [ms]: {}/{}/{}", DECIMAL_FORMAT.format(minQueryLatency / 1000000f),
				DECIMAL_FORMAT.format(maxQueryLatency / 1000000f), DECIMAL_FORMAT.format(avgQueryLatency / 1000000f));
	}

	private static List<List<String>> createSlices(String[] mongoUri, int numThreads) {
		final List<List<String>> slices = new ArrayList<List<String>>(numThreads);
		if (mongoUri.length >= numThreads) {
			for (int i = 0; i < numThreads; i++) {
				slices.add(new ArrayList<String>());
			}
			for (int i = 0; i < mongoUri.length; i++) {
				int sliceIdx = i % numThreads;
				slices.get(sliceIdx).add(mongoUri[i]);
			}
		} else {
			createSlicesForThreads(mongoUri, numThreads, slices);
		}
		int count = 0;
		for (List<String> uriTmp : slices) {
			LOG.info("Thread {} will connect to {}", count++, uriTmp);
		}
		return slices;
	}

	private static void createSlicesForThreads(String[] mongoUri, int numThreads,
			final List<List<String>> slices) {
		int portIndex = 0;
		for (int currentThread = 0; currentThread < numThreads; currentThread++) {
			final List<String> conTmp;
			if (slices.size() <= currentThread) {
				conTmp = new ArrayList<>();
				slices.add(currentThread, conTmp);
			} else {
				conTmp = slices.get(currentThread);
			}
			conTmp.add(mongoUri[portIndex++]);
			if (portIndex == mongoUri.length) {
				portIndex = 0;
			}
		}
	}


	private void doLoadPhase(String[] mongoUri, int numThreads, int numDocuments, 
			int dockSize, int timeouts, DocType docType, boolean skipDrop, int insertOffset) {
		final Map<LoadThread, Thread> threads = new HashMap<LoadThread, Thread>(numThreads);
		final List<List<String>> slices = createSlices(mongoUri, numThreads);

		// If there are multiple threads going to write to the same database,
		// then we need to distribute the records among the threads for
		// inserting into the database
		int[] startRecord = new int[numThreads];
		int[] endRecord = new int[numThreads];

		if (numThreads > mongoUri.length) { // more threads than DBs
			determinStartAndEndIndices(mongoUri, numDocuments, insertOffset,
					slices, startRecord, endRecord);
		} else {    // more DBs than threads
			Arrays.fill(startRecord, 0+insertOffset);
			Arrays.fill(endRecord, numDocuments-1);
		}

		for (int currentThread = 0; currentThread < numThreads; currentThread++) {
			LOG.info("Thread {} will insert records {} to {}", currentThread, startRecord[currentThread], endRecord[currentThread]);
			LoadThread l = new LoadThread(currentThread, slices.get(currentThread), dockSize, 
					timeouts, startRecord[currentThread], endRecord[currentThread], docType, skipDrop);
			threads.put(l, new Thread(l));
		}

		for (Thread t : threads.values()) {
			t.start();
		}
		for (Thread t : threads.values()) {
			if (t.isAlive()) {
				try {
					Thread.sleep(100);
				} catch (InterruptedException e) {
					LOG.error("Error while waiting for thread", e);
				}
			}
		}
	}

	private  void determinStartAndEndIndices(String[] mongoUri,
			int numDocuments, int insertOffset,
			final List<List<String>> slices, int[] startRecord, int[] endRecord) {
		for (int currentDatabaseIndex = 0; currentDatabaseIndex < mongoUri.length; currentDatabaseIndex++) { // for each DB
			List<Integer> threadsForDb = new ArrayList<Integer>();
			for (int currentThread = 0; currentThread < slices.size(); currentThread++) {   // for each thread
				List<String> subSlice = slices.get(currentThread);
				for (int k = 0; k < subSlice.size(); k++) { // for each assigned DB
					if (mongoUri[currentDatabaseIndex].equals(subSlice.get(k))) {   // the current DB is served by the current thread
						threadsForDb.add(currentThread);
					}
				}
			}
			createBatchIndicies(numDocuments, insertOffset, startRecord,
					endRecord, threadsForDb);
			endRecord[threadsForDb.get(threadsForDb.size()-1)] = numDocuments-1;
		}
	}

	private  void createBatchIndicies(int numDocuments, int insertOffset,
			int[] startRecord, int[] endRecord, List<Integer> threadsForDb) {
		int documentsPerThread = numDocuments/threadsForDb.size();
		int insertStart = insertOffset;
		startRecord[threadsForDb.get(0)] = insertStart;
		for(int m = 0; m < threadsForDb.size()-1; m++) {    // fill start and end records
			endRecord[threadsForDb.get(m)] = insertStart+documentsPerThread-1-insertOffset;
			insertStart += documentsPerThread;
			startRecord[threadsForDb.get(m+1)] = insertStart;
		}
	}

	private static  void showHelp(final Options ops) {
		final StringBuilder header = new StringBuilder();
		header.append("\nOptions:");
		final StringBuilder footer = new StringBuilder();
		footer.append("\nThe benchmark is split into two phases: Load and Run. ")
		.append("Random data is added during the load phase which is in turn retrieved from mongodb in the run phase");
		String jarName;
		try {
			jarName = MongoBench.class.getProtectionDomain().getCodeSource().getLocation().toURI().getPath();
			int posSlash = jarName.lastIndexOf('/');
			if (posSlash != -1 && jarName.length() > posSlash) {
				jarName = jarName.substring(posSlash + 1);
			}
		} catch (URISyntaxException e) {
			jarName = "mongo-bench.jar";
		}
		new HelpFormatter().printHelp(120, "java -jar " + jarName + " [options]", header.toString(), ops, footer.toString());
	}

}
