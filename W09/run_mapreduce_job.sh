#!/bin/bash
/usr/bin/hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar \
-files mapper.py,reducer.py \
-input /mapreduce/averages/test.txt \
-output /mapreduce/averages/output01 \
-mapper mapper.py \
-reducer reducer.py 

