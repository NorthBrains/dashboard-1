#!/bin/bash

TABLE="company_one.sales_data"
RECORD_LIMIT=500

CQL_COUNT="SELECT count(*) FROM $TABLE;"
COUNT=$(cqlsh -u cassandra -p cassandra -e "$CQL_COUNT" | awk 'NR==4{print $1}')

if [ "$COUNT" -gt "$RECORD_LIMIT" ]; then
  DELETE_COUNT=$((COUNT - RECORD_LIMIT))
  CQL_DELETE="DELETE FROM $TABLE WHERE record_id IN (SELECT record_id FROM $TABLE ORDER BY timestamp ASC LIMIT $DELETE_COUNT);"
  cqlsh -e "$CQL_DELETE"

  echo "$(date): Deleted $DELETE_COUNT records from $TABLE, keeping the latest $RECORD_LIMIT records." >> /crons/cron_del_sales_data.sh
else
  echo "$(date): No records deleted, only $COUNT records present in $TABLE" >> /crons/cron_del_sales_data.sh
fi