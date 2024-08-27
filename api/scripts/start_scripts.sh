#!/bin/bash

echo "Waiting 50 sec."
sleep 50

echo "Running scripts now."

echo "Starting sales_sales.py"
python3 /scripts/sales_script.py > /scripts/sales_script.log 2>&1 &
echo "sales_sales.py started"

echo "Starting warehouse_script.py"
python3 /scripts/warehouse_script.py > /scripts/warehouse_script.log 2>&1 &
echo "warehouse_script.py started"

tail -f /dev/null
