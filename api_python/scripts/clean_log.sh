#!/bin/bash

LOG_FILE_SALES="/scripts/sales_script.log"
LOG_FILE_WAREHOUSE="/scripts/warehouse_script.log"

echo "" > $LOG_FILE_SALES
echo "$(date): Log file cleared" >> $LOG_FILE_SALES

echo "" > $LOG_FILE_WAREHOUSE
echo "$(date): Log file cleared" >> $LOG_FILE_WAREHOUSE
