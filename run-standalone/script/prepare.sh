hdfs dfs -mkdir /user

hdfs dfs -mkdir /user/hive


hdfs dfs -mkdir -p /public-dataset/hive/listings
hdfs dfs -mkdir -p /public-dataset/hive/stations

if [ ! -f ./stations.jsonl ]; then
    wget https://eddb.io/archive/v5/stations.jsonl
fi
if [ ! -f ./listings.csv ]; then
    wget https://eddb.io/archive/v5/listings.csv
fi

hdfs dfs -put listings.csv /public-dataset/hive/listings
hdfs dfs -put stations.jsonl /public-dataset/hive/stations


hdfs dfs -mkdir -p /project/sample
hdfs dfs -mkdir -p /project/sample/hive/default

beeline -u jdbc:hive2://localhost:20103/default -n hive -f prepare.hql