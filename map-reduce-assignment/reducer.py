import sys

# Initialize variables to keep track of total and count of numeric values
total = 0
count = 0

# Loop through each line of input
for line in sys.stdin:
    try:
        # Convert the line to a numeric value by setting input as int value only
        numeric_value = int(line.strip())
        
        # Update the total and count
        total += numeric_value
        count += 1
    except ValueError:
        # Skip lines that are not numeric values
        continue

# Calculate the average
if count > 0:
    average = float(total) / count
    # print 'AVERAGE = %.2f' % average
    print("AVERAGE = %.2f" % average)
else:
    # print 'AVERAGE = No numeric values found.'
    print("AVERAGE = No numeric values found.")