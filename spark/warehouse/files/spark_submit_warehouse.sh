#!/bin/bash
sleep 100

$SPARK_HOME/sbin/start-worker.sh spark://172.30.0.2:7077 &

spark-submit \
  --master spark://172.30.0.2:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,com.datastax.spark:spark-cassandra-connector_2.12:3.5.1 \
  --driver-memory 512m \
  --driver-cores 1 \
  --executor-memory 512m \
  --executor-cores 1 \
  --conf spark.task.cpus=1 \
  --conf spark.executor.cores=1 \
  --conf spark.dynamicAllocation.enabled=true \
  /files/warehouse_stream.py