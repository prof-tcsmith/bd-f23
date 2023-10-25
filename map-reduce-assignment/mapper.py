import sys
import re

# Regular expression pattern to match numeric values
pattern = r'\d+'

# Loop through each line of input
for line in sys.stdin:
    # Use regular expression to find all numeric values in the line
    numeric_values = re.findall(pattern, line)
    
    # Print each numeric value separately to stdout
    for numeric_value in numeric_values:
        # print '%s' % numeric_value
        print(numeric_value)