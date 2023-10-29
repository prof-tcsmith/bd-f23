#!/bin/bash
/usr/bin/hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar \
-files mapper.py,reducer.py \
-input /mapreduce/word-count/test.txt \
-output /mapreduce/word-count/output01 \
-mapper mapper.py \
-reducer reducer.py 

