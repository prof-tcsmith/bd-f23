#!/bin/bash
/usr/bin/hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar \
-files mapper.py,reducer.py \
-input /mapreduce/average/test.txt \
-output /mapreduce/average/output01 \
-mapper mapper.py \
-reducer reducer.py 

