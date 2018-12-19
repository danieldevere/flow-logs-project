#!/bin/bash

LOGS_BUCKET=$1
TEMP_BUCKET=$2

# create initial tables
hive --hiveconf a=$LOGS_BUCKET --hiveconf b=$TEMP_BUCKET -f flow_logs.hql

# remember time
TIME=$(date +%s)
# run inserts
hive -f batch.hql
yarn jar /home/hadoop/uber-jar.jar edu.uchicago.ddevere.IpAddressByMinute.IpAddressByMinute "s3://$TEMP_BUCKET/orc-tables/ip_traffic/" "s3://$TEMP_BUCKET/sequence-files/ip_traffic_by_minute/"

# clean hbase table


hive --hiveconf a=$LOGS_BUCKET --hiveconf b=$TEMP_BUCKET -f hbase.hql

hbase shell hbase_swap_tables.txt 

# most recent batch finish
curl -X GET "http://localhost:8000/updateTime?time=$TIME"
echo $TIME > /home/hadoop/time.txt

# clean up data
hive -f cleanup.hql
aws s3 rm "s3://$TEMP_BUCKET/sequence-files/" --recursive
aws s3 rm "s3://$TEMP_BUCKET/orc-tables/ip_traffic/" --recursive
aws s3 rm "s3://$TEMP_BUCKET/orc-tables/ip_traffic_\$folder\$"