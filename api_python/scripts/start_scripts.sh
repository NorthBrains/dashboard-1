#!/bin/bash

python3 /scripts/sales_script.py > /scripts/sales_script.log 2>&1
python3 /scripts/warehouse_script.py > /scripts/warehouse_script.log 2>&1
