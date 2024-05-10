#!/bin/bash

#delete the oldest records from Cassandra
CQL="DELETE FROM products_data.data WHERE id = ? AND record_id IN (SELECT record_id FROM products_data.data WHERE id = ? ORDER BY timestamp ASC LIMIT (SELECT count(*) FROM products_data.data WHERE id = ?) - 300)"

cqlsh -e "$CQL"