#!/usr/bin/env python

import sys
import string

# Input comes from standard input
for line in sys.stdin:
    line = line.strip()
    values = line.split()

    # Process each value in the line
    for value in values:
        # Check if the value is numeric
        if all(char in string.digits + '.' for char in value):
            # Convert the value to a float
            numeric_value = float(value)
            # Emit the numeric value as the key with a count of 1
            print '%s\t%s' % (numeric_value, 1)