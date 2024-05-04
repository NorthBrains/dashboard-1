#!/bin/bash
set -e

#wytworzenie keyspace w Cassandrze (if not exists)
cqlsh -u cassandra -p cassandra -f /docker-entrypoint-initdb.d/init.cql