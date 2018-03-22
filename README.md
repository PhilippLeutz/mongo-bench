# Tiny MongoDB Benchmark

This is a benchmark for assessing MongoDB performance in a SaaS environment when running a lot of instances on a single box/VM. The benchmark is split into a load and a run phase. During the load phase, random data is inserted into MongoDB, during the run phase the data is retrieved and new random data is inserted into MongoDB with a read/write ratio of 0.9.

## Building

The benchmark is a Java project build with Apache Maven. In order to build the project from source run the package phase of Apache Maven:
```bash
#> mvn package
```

## Usage

The benchmark is written in Java and at least Java version 6 is required to run the generated jar file. Since a lot of latency data is recorded per thread, large amounts of memory are needed and the benchmark has to be run with a larger than normal heapsize during the run phase.
usage: java -jar mongo-bench-1.1-beta-SNAPSHOT-jar-with-dependencies.jar
[options]

    Options:
     -a,--record-latencies <arg>     Set the file prefix to which to write latencies to of all the DBs
     -c,--num-documents <arg>        The number of documents to create during the load phase
     -d,--duration <arg>             Run the bench for this many seconds
     -e,--user <arg>                 Username for authentication
     -f,--connect-file <arg>         Use a connection file with MongoDB URIs instead of p and t
     -h,--help                       Show this help dialog
     -i,--replica-set <arg>          Name of the replica set to connect
     -j,--target-rate <arg>          Send request at the given rate. Accepts decimal numbers
     -k,--password <arg>             Password for authentication
     -l,--phase <arg>                The phase to execute [run|load]
     -n,--num-thread <arg>           The number of threads to run
     -o,--timeout <arg>              Set the timeouts in seconds for networking operations
     -p,--port <arg>                 The ports to connect to
     -r,--reporting-interval <arg>   The interval in seconds for reporting progress
     -s,--document-size <arg>        The size of the created documents
     -t,--target  <arg>              The target single host to connect to
     -u,--ssl                        Use SSL for MongoDB connections
     -w,--warmup-time <arg>          The number of seconds to wait before actually collecting result data
    
    The benchmark is split into two phases: Load and Run. Random data is added
    during the load phase which is in turn retrieved from mongodb in the run phase.

### Examples
Loading `1000` documents of size `1024` bytes each into MongoDB instances on ports `30001-30010` on `9.114.14.14` using `8` threads:
```
#> java -jar /tmp/mongo-bench-1.0-SNAPSHOT-jar-with-dependencies.jar -s 1024 \
-c 1000 -l load -p 30001-30010 -t 9.114.14.14 -n 8
```

---
Loading `1000` documents of size `1024` bytes into a MongoDB instance on port
`20401` on `10.1.9.199` using `1` thread, replica-set name `usman1`, SSL and
authorization username and password `usman`:
```
#> java -Djavax.net.ssl.trustStore=/home/usman/cluster_keystore.jks \
-Djavax.net.ssl.trustStorePassword=changeit -Djavax.net.ssl.debug=all -jar \
target/mongo-bench-1.1-beta-SNAPSHOT-jar-with-dependencies.jar -s 1024 -c 1000 \
-l load -p 20401 -t 10.1.9.199 -n 1 -u -e usman -k usman -i usman1
```

Of course, you would need to import the certificate and add it to a `keystore`,
for java to read. 

---
Using a "connect file" as given below:
```
mongodb://usman:usman@IP9-132-34.ibm.com:2058,IP9-171-13.ibm.com:2038,IP9-191-15.ibm.com:2319/?replicaSet=usman2&ssl=true
mongodb://usman:usman@IP9-132-34.ibm.com:1371,IP9-171-13.ibm.com:2088,IP9-191-15.ibm.com:2919/?replicaSet=usman1&ssl=true
```

One can run the following command to load into the two databases:
```
java -Djavax.net.ssl.trustStore=/home/usman/cluster_keystore.jks \
-Djavax.net.ssl.trustStorePassword=changeit -jar \
target/mongo-bench-2.0-beta-SNAPSHOT-jar-with-dependencies.jar -s 1024 -c 1000 \
-l load -n 2 -f /home/usman/cluster_connect.txt
```

Currently, use only 1 thread for each DB for loading.

For running the read/write test for 60 seconds, use the following command:
```
java -Djavax.net.ssl.trustStore=/home/usman/cluster_keystore.jks \
-Djavax.net.ssl.trustStorePassword=changeit -jar \
target/mongo-bench-2.0-beta-SNAPSHOT-jar-with-dependencies.jar -s 1024 -c 1000 \
-l run -n 2 -f /home/usman/cluster_connect.txt -d 60
```

---
Running the benchmark against ports `30001-30010` on the box `9.114.14.14` using 
4 threads for `600` seconds with a `60` second warmup time and target an overall
rate of `1000` transactions/second:
```
#> java -Xmx16384m -jar /tmp/mongo-bench-1.0-SNAPSHOT-jar-with-dependencies.jar \
-l run -p 30001-30010 -t 9.114.14.14 -n 4 -w 60 -d 600 -j 1000
```


### Debugging
For debugging the application, start the JVM in terminal via:
```
java -Djavax.net.ssl.trustStore=/home/usman/cluster_keystore.jks \
-Djavax.net.ssl.trustStorePassword=changeit \
-agentlib:jdwp=transport=dt_socket,address=8000,server=y,suspend=y -jar \
target/mongo-bench-2.0-beta-SNAPSHOT-jar-with-dependencies.jar -s 1024 -c 1000 \
-l load -n 2 -f /home/usman/cluster_connect.txt -d 60
```

In a separate terminal, use `jdb` to attach to this JVM.
```
jdb -connect com.sun.jdi.SocketAttach:port=8000 -sourcepath src/main/java/
``` 

---
To connect to a DB, use the following commands:
```
mongo
"mongodb://usman:usman@IP9-114-15.pok.stglabs.ibm.com:20359,IP9-114-13.pok.stglabs.ibm.com:20598,IP9-114-14.pok.stglabs.ibm.com:20438/?replicaSet=usman2&ssl=true" \
--sslCAFile /tmp/IP9-114-15.pok.stglabs.ibm.com:20359.pem --ssl \
--sslAllowInvalidCertificates
```

Afterwards, use the following commands to check the status of the DB.
```
rs.status();
show dbs;
use mongo-bench;
db.stats();
```
