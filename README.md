# SparkRDMA ShuffleManager Plugin
SparkRDMA is a high performance ShuffleManager plugin for Apache Spark that uses RDMA (instead of TCP) when
performing Shuffle data transfers in Spark jobs.

This open-source project is developed, maintained and supported by [Mellanox Technologies](http://www.mellanox.com).

## Performance results
### Terasort
![TeraSort results](https://user-images.githubusercontent.com/1121987/44670087-6c78bb00-aa2a-11e8-834c-71bc177abd87.png)

Running 320GB TeraSort workload with SparkRDMA is x2.63 faster than standard Spark (runtime in seconds)

Test environment:

7 Spark standalone workers on Azure "h16mr" VM instance,  Intel Haswell E5-2667 V3,

224GB RAM, 2000GB SSD for temporary storage, Mellanox InfiniBand FDR (56Gb/s)

Also featured at the Spark+AI Summit 2018, please see more info on our session:
https://databricks.com/session/accelerated-spark-on-azure-seamless-and-scalable-hardware-offloads-in-the-cloud

### Pagerank
![PageRank results](https://user-images.githubusercontent.com/1121987/44669579-ec058a80-aa28-11e8-8ecf-4a66134021e6.png)

Running 19GB Pagerank with SparkRDMA is x2.01 faster than standard Spark (runtime in seconds)

Test environment:

5 Spark standalone workers, 2x Intel Xeon E5-2697 v3 @ 2.60GHz, 25 cores per Worker, 150GB RAM, non-flash storage (HDD)

Mellanox ConnectX-5 network adapter with 100GbE RoCE fabric, connected with a Mellanox Spectrum switch

## Wiki pages
For more information on configuration, performance tuning and troubleshooting, please visit the [SparkRDMA GitHub Wiki](https://github.com/Mellanox/SparkRDMA/wiki)

## Runtime requirements
* Apache Spark 2.0.0/2.1.0/2.2.0/2.3.0/2.4.0
* Java 8
* An RDMA-supported network, e.g. RoCE or Infiniband

## Installation

### Obtain SparkRDMA and DiSNI binaries
Please use the ["Releases"](https://github.com/Mellanox/SparkRDMA/releases) page to download pre-built binaries.
<br>If you would like to build the project yourself, please refer to the ["Build"](https://github.com/Mellanox/SparkRDMA#build) section below.

The pre-built binaries are packed as an archive that contains the following files:
* spark-rdma-3.1-for-spark-2.0.0-jar-with-dependencies.jar
* spark-rdma-3.1-for-spark-2.1.0-jar-with-dependencies.jar
* spark-rdma-3.1-for-spark-2.2.0-jar-with-dependencies.jar
* spark-rdma-3.1-for-spark-2.3.0-jar-with-dependencies.jar
* spark-rdma-3.1-for-spark-2.4.0-jar-with-dependencies.jar
* libdisni.so

libdisni.so **must** be in `java.library.path` on every Spark Master and Worker (usually in /usr/lib)

### Configuration

Provide Spark the location of the SparkRDMA plugin jars by using the extraClassPath option.  For standalone mode this can
be added to either spark-defaults.conf or any runtime configuration file.  For client mode this **must** be added to spark-defaults.conf. For Spark 2.0.0 (Replace with 2.1.0, 2.2.0, 2.3.0, 2.4.0 according to your Spark version):
```
spark.driver.extraClassPath   /path/to/SparkRDMA/target/spark-rdma-3.1-for-spark-2.0.0-jar-with-dependencies.jar
spark.executor.extraClassPath /path/to/SparkRDMA/target/spark-rdma-3.1-for-spark-2.0.0-jar-with-dependencies.jar
```

### Running

To enable the SparkRDMA Shuffle Manager plugin, add the following line to either spark-defaults.conf or any runtime configuration file:

```
spark.shuffle.manager   org.apache.spark.shuffle.rdma.RdmaShuffleManager
```

## Build

Building the SparkRDMA plugin requires [Apache Maven](http://maven.apache.org/) and Java 8

1. Obtain a clone of [SparkRDMA](https://github.com/Mellanox/SparkRDMA)

2. Build the plugin for your Spark version (either 2.0.0, 2.1.0, 2.2.0, 2.3.0, 2.4.0), e.g. for Spark 2.0.0:
```
mvn -DskipTests clean package -Pspark-2.0.0
```

3. Obtain a clone of [DiSNI](https://github.com/zrlio/disni) for building libdisni:

```
git clone https://github.com/zrlio/disni.git
cd disni
git checkout tags/v1.7 -b v1.7
```

4. Compile and install only libdisni (the jars are already included in the SparkRDMA plugin):

```
cd libdisni
autoprepare.sh
./configure --with-jdk=/path/to/java8/jdk
make
make install
```

## Community discussions and support

For any questions, issues or suggestions, please use our Google group:
https://groups.google.com/forum/#!forum/sparkrdma

## Contributions

Any PR submissions are welcome
