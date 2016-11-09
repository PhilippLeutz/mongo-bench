package com.ibm.mongo;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import org.apache.commons.lang.RandomStringUtils;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DecimalFormat;
import java.util.*;

public class LoadThread implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(LoadThread.class);

    private final String host;
    private final List<Integer> ports;
    private final int numDocuments;
    private final int docSize;
    private final int maxBatchSize = 1000;
    private final int timeoutMs;
    private final Map<String, Integer> failed = new HashMap<>();

    private final static DecimalFormat decimalFormat = new DecimalFormat("0.0000");

    public LoadThread(String host, List<Integer> ports, int numDocuments, int docSize, int timeout) {
        this.host = host;
        this.ports = ports;
        this.numDocuments = numDocuments;
        this.docSize = docSize;
        this.timeoutMs = timeout * 1000;
    }

    @Override
    public void run() {
        log.info("Loading data into {} instances on {}", ports.size(), host);
        for (int i = 0; i < ports.size(); i++) {
            int count = 0, currentBatchSize;
            final MongoClientOptions ops = MongoClientOptions.builder()
                    .maxWaitTime(timeoutMs)
                    .connectTimeout(timeoutMs)
                    .socketTimeout(timeoutMs)
                    .heartbeatConnectTimeout(timeoutMs)
                    .serverSelectionTimeout(timeoutMs)
                    .build();
            final MongoClient client = new MongoClient(new ServerAddress(host, ports.get(i)), ops);
            for (final String name : client.listDatabaseNames()) {
                if (name.equalsIgnoreCase(MongoBench.DB_NAME)) {
                    log.warn("Database {} exists and will be purged before inserting", MongoBench.DB_NAME);
                    client.dropDatabase(name);
                    break;
                }
            }
            long startLoad = System.currentTimeMillis();
            while (count < numDocuments) {
                currentBatchSize = numDocuments - count > maxBatchSize ? maxBatchSize : numDocuments - count;
                final Document[] docs = createDocuments(currentBatchSize, count);
                final MongoCollection<Document> collection = client.getDatabase(MongoBench.DB_NAME).getCollection(MongoBench.COLLECTION_NAME);
                try {
                    collection.insertMany(Arrays.asList(docs));
                } catch (Exception e) {
                    log.warn("Error while inserting {} documents at {}:{}", currentBatchSize, host, ports.get(i));
                    failed.put(host + ":" + ports.get(i), numDocuments - count);
                    break;
                }
                count += currentBatchSize;
            }
            client.close();
            long duration = System.currentTimeMillis() - startLoad;
            float rate = 1000f * 1000f / (float) duration;
            if (failed.size() > 0) {
                int numFailed = 0;
                log.error("Errors occured during the loading of the data");
                for (final Map.Entry<String, Integer> error : failed.entrySet()) {
                    log.error("Unable to insert {} documents at {}:{}", error.getValue(), error.getKey());
                    numFailed+=error.getValue();
                }
                log.error("Overall {} inserts failed", numFailed);
            }
            log.info("Finished loading {} documents in {}:{} [{} inserts/sec]", count, host, ports.get(i), rate);
        }
    }

    private Document[] createDocuments(int count, int offset) {
        final String data = RandomStringUtils.randomAlphabetic(docSize);
        final Document[] docs = new Document[count];
        for (int i = 0; i < count; i++) {
            docs[i] = new Document()
                    .append("_id", i + offset)
                    .append("data", data);
        }
        return docs;
    }
}
