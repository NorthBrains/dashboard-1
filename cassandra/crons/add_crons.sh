#!/bin/bash

echo "*/45 * * * * root /crons/cron_del_sales_data.sh >> /crons/cron_del_sales_data.log 2>&1" > /etc/cron.d/cron_del_sales_data
chmod +x /crons/cron_del_sales_data.sh
chmod 0644 /etc/cron.d/cron_del_sales_data

echo "*/45 * * * * root /crons/cron_del_warehouse_data.sh >> /crons/cron_del_warehouse_data.log 2>&1" > /etc/cron.d/cron_del_warehouse_data
chmod +x /crons/cron_del_warehouse_data.sh
chmod 0644 /etc/cron.d/cron_del_warehouse_data

service cron start

echo "*/45 * * * * /crons/cron_del_sales_data.sh >> /crons/cron_del_sales_data.log 2>&1" >> /etc/crontab
echo "*/45 * * * * /crons/cron_del_warehouse_data.sh >> /crons/cron_del_warehouse_data.log 2>&1" >> /etc/crontab

touch /var/log/cron_sales.log /var/log/cron_warehouse.log