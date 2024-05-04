#!/bin/bash
bin/kafka-topics.sh --create --topic products_data --partitions 1 --replication-factor 1 -bootstrap-server localhost:9092