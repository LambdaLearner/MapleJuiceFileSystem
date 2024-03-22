#!/usr/bin/python3
import sys

if __name__ == "__main__":
    # Return (1, (detection_field_value, frequency))
    for line in sys.stdin:
        detection, freq = line.split('\t')
        det_tuple = (detection, freq)
        print('%s\t%s' % (1, det_tuple))