# Run in Standalone

To test Scuba, you can use a standalone Hadoop cluster using https://github.com/jetoile/hadoop-unit

## Start your cluster

Make sure to activate the following components in $HADOOP_UNIT_STANDALONE_HOME/conf/hadoop.properties

```
hdfs=true
zookeeper=true
yarn=true
hivemeta=true
hiveserver2=true
```

The run the standalone cluster:

```
$HADOOP_UNIT_STANDALONE_HOME/bin/hadoop-unit-standalone console
```

## Install & configure HDFS / Hive clients

### Hadoop

Download Hadoop 2.7.3 from https://archive.apache.org/dist/hadoop/common/hadoop-2.7.3/

Unzip it. Set your environment variables:

```
# JAVA_HOME must already be set
export JAVA_HOME=...
export PATH=$JAVA_HOM/bin:$PATH

# Set Hadoop env
export HADOOP_HOME=$HOOME/hadoop-2.7.3
export PATH=$HADOOP_HOME/bin:$PATH
```

Set the Hadoop configuration to use your Standalone cluster. Change the $HADOOP_HOME/etc/hadoop/core-site.xml:

```
    <property>
      <name>fs.defaultFS</name>
      <value>hdfs://localhost:20112</value>
    </property>
```

Test that HDFS CLI is working:

```
hdfs dfs -mkdir /user
hdfs dfs -ls /
```

### Hive

Download Hive 1.2.1 from https://archive.apache.org/dist/hive/hive-1.2.1/

Unzip it. Set your environment variables:

```
# Set Hive env
export HIVE_HOME=$HIVE/apache-hive-1.2.1-bin
export PATH=$HIVE_HOME/bin:$PATH
```

Test that Hive CLI is working:

```
beeline -u jdbc:hive2://localhost:20103/default -n hive

Connecting to jdbc:hive2://localhost:20103/default
log4j:WARN No appenders could be found for logger (org.apache.hive.jdbc.Utils).
log4j:WARN Please initialize the log4j system properly.
log4j:WARN See http://logging.apache.org/log4j/1.2/faq.html#noconfig for more info.
Connected to: Apache Hive (version 1.2.1000.2.6.1.0-129)
Driver: Spark Project Core (version 1.6.2)
Transaction isolation: TRANSACTION_REPEATABLE_READ
Beeline version 1.6.2 by Apache Hive
0: jdbc:hive2://localhost:20103/default>
```

## Prepare sample data

In order to test the application, you may need some sample data.
Please run the following script:

```
bash ./prepare.sh
```

It will create 2 Hive Databases:

* **public_dataset** : that contains **listings** and **stations** tables, you can use them as input table to build your dataset.
* **project_sample** : that will be used for your working project to store your custom dataset.

## Run the API

Use the "dev" profile to run with Hadoop Unit configuration.

Use the MAVEN_OPTS variable to be able to debug the API.

```
export MAVEN_OPTS=-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005
mvn -P dev spring-boot:run
```

If you want to run the API with the Web UI embedded, use also the "web" profile:

```
mvn -P dev,web spring-boot:run
```

## Run the Web UI separately

I case you run the API without the Web UI embedded, run the Web UI using npm:

```
cd datalake-toolbox-web
npm install
npm run serve
```

## Initial setup

First, you need to refresh the list of public dataset using the API.

Go to the URL http://localhost:8000/swagger-ui.html#!/data-set-resource/refreshUsingPOST and run the REST endpoint. 