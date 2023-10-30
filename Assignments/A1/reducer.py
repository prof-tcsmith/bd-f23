#!/usr/bin/env python
from operator import itemgetter
import sys

total = 0
count = 0

# Input comes from standard input
for line in sys.stdin:
    line = line.strip()
    key, value = line.split('\t')
    numeric_value = float(key)
    count += int(value)
    total += numeric_value

# Calculate the average
if count > 0:
    average = total / count
    print "AVERAGE=%.2f" % average
else:
    print "No numeric values found"