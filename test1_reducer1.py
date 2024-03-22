#!/usr/bin/python3
import sys

if __name__ == "__main__":
    detection_freq = {}
    for line in sys.stdin:
        detection, _ = line.split('\t')
        detection_freq[detection] = detection_freq.get(detection, 0) + 1   
    # print the (detection field, freq)
    for d in detection_freq:    
        print('%s\t%s' % (d, detection_freq[d]))

