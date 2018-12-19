#!/bin/bash

while : 
do 
    java -Xmx300m -jar /home/hadoop/hbase-rest.jar $(cat /home/hadoop/time.txt)
    echo $(date) >> restart.txt
    sleep 1
done
