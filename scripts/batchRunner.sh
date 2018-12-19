#!/bin/bash

while :
do
	./batch.sh "s3://dan-flow-logs/AWSLogs/" "dan-sequence-files"
	sleep 3600
done