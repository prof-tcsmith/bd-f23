#!/bin/bash
/usr/bin/hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar \
-files mapper.py,reducer.py \
-input /mapreduce/avg/data.txt \
-output /mapreduce/avg/output03 \
-mapper mapper.py \
-reducer reducer.py 
