#!/bin/bash

echo "Waiting for Cassandra to be available at 172.30.0.11:9042..."

until cqlsh 172.30.0.11 9042 -u cassandra -p cassandra -e 'describe keyspaces'; do
  echo "Cassandra is unavailable - sleeping"
  sleep 10
done

echo "Cassandra is up - executing CQL script..."
cqlsh 172.30.0.11 9042 -u cassandra -p cassandra -f /initdb/init.cql

echo "CQL script executed successfully."