#!/usr/bin/env python

import sys

#initializing variables to track total sum and total count
current_key = None
total_sum = 0
total_count = 0


for line in sys.stdin:                                  #iterate each line being recieved from the mapper
    line = line.strip()                                 #strip leading and trailing whitespace
    key, value = line.split('\t')                       #split into key and value using tab as separator/splitter
    value_sum, count = map(float, value.split(','))     #split the value into value_sum and count using map function (while also converting them into floating points)

    total_sum += value_sum  # calculate the total sum by adding all values
    total_count += count    # calculate the total count by adding number of numbers

if total_count > 0:                         #confirm that we have some data and calculate the average 
    average = total_sum / total_count
    rounded_avg = round(average, 2)         #limit the output to 2 decimal spaces
    print 'AVERAGE=%.2f' % rounded_avg      #using .2 to specify the precision for floating point number

