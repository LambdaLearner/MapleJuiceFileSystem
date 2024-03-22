#!/usr/bin/python3
import sys


if __name__ == "__main__":
    x = sys.argv[1]
    interc_idx = 10
    det_idx = 9
    for line in sys.stdin:
        row = line.strip().split(',')
        interconne = row[interc_idx]
        detection = row[det_idx]
        if interconne == x:
            print('%s\t%s' % (detection, 1))
