#!/usr/bin/python3
import sys

# identity
if __name__ == "__main__":
    for line in sys.stdin:
        red_line = line.split('\t')[1]
        print('%s' % (red_line))
