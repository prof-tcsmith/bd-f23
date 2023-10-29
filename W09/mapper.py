#!/usr/bin/env python

import sys

for line in sys.stdin: # look through data that is piped into this program
    line = line.strip() # remove trailing and leading whitespaces
    numbers = line.split()  #split each string and store in numbers variable

    sum = 0
    count = 0

    for number in numbers:      #loop through all strings in the file
        try:            
            num = float(number) #inside this try block, check all strings and convert it to a float 
            sum += num          # if no error is encountered, add this number to sum    
            count += 1          # increment the counter so that we can use it for dividing later
        except ValueError:      # if a string is encountered that cannot be converted into a floating point number (for example text string), then it will throw a valueError and get caught in this except block
            continue            # ignore the character string and keep the loop going

    if count > 0: 
        print '%s\t%s,%s' % ("AVERAGE", sum, count) #after each line is looped, print the key("AVERAGE"), sum, and count of numbers in that line

