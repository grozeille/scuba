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
mvn spring-boot:run -pl scuba-api -amd -P dev
```

If you want to run the API with the Web UI embedded, use also the "web" profile:

```
mvn spring-boot:run -pl scuba-api -amd -P dev,web
```

## Run the Web UI separately

I case you run the API without the Web UI embedded, run the Web UI using npm:

```
cd scuba-web
npm install
npm run serve
```

## Initial setup

### Setup admin account

By default, 2 users are allowed:

- admin
- user

You can change it to use Oauth2 provider.

First, you need to register the user "admin" as an administrator. Retrieve the admin token from the application.yml or from the console log:

![Admin Token](https://raw.githubusercontent.com/grozeille/scuba/master/run-standalone/img/default-admin-token.png)

If you started the Web UI using npm, go to the URL: http://localhost:3000/#!/setup, enter the following login/password:

- login: admin
- password: admin

And enter this token:

![Setup Admin Token](https://raw.githubusercontent.com/grozeille/scuba/master/run-standalone/img/setup-token.png)

Now the current account **admin** is member of administrators. 

### Create a project

You can do anything unless you selected the current project for the current user. 

First, to create the project, go to the URL: http://localhost:3000/#!/admin

Enter the following information to create a new project:

- New Project: sample
- Hive Database: project_sample
- HDFS Working Directory: /project/sample

![Create project](https://raw.githubusercontent.com/grozeille/scuba/master/run-standalone/img/create-project.png)

Now the project is created, click on it to add **admin** as member. Don't forget to save.

![Add member](https://raw.githubusercontent.com/grozeille/scuba/master/run-standalone/img/add-member.png)

At last, you have to select this project as your default one for the current user. Go to the URL http://localhost:3000/#!/profile and select the created project.

![Select project](https://raw.githubusercontent.com/grozeille/scuba/master/run-standalone/img/add-member.png)

### Refresh public hive tables

The preparation script created tables in **public_dataset** database. To use them, you need to refresh the list of public dataset using the API.

Go to the URL http://localhost:8800/swagger-ui.html#!/data-set-resource/refreshUsingPOST and trigger the endpoint.

![Refresh Dataset](https://raw.githubusercontent.com/grozeille/scuba/master/run-standalone/img/select-project.png)

You should see now the dataset in the UI.

### Create new datasets

Now everything is setup to let you create new datasets.

![Demo 01](https://raw.githubusercontent.com/grozeille/scuba/master/run-standalone/img/demo-01.png)

![Demo 02](https://raw.githubusercontent.com/grozeille/scuba/master/run-standalone/img/demo-02.png)

![Demo 03](https://raw.githubusercontent.com/grozeille/scuba/master/run-standalone/img/demo-03.png)

![Demo 04](https://raw.githubusercontent.com/grozeille/scuba/master/run-standalone/img/demo-04.png)

![Demo 05](https://raw.githubusercontent.com/grozeille/scuba/master/run-standalone/img/demo-05.png)

![Demo 06](https://raw.githubusercontent.com/grozeille/scuba/master/run-standalone/img/demo-06.png)

![Demo 07](https://raw.githubusercontent.com/grozeille/scuba/master/run-standalone/img/demo-07.png)

![Demo 08](https://raw.githubusercontent.com/grozeille/scuba/master/run-standalone/img/demo-08.png)