#!/usr/bin/python3
import sys
import ast
if __name__ == "__main__":
    # input = (1, (detection_field_value, frequency))
    # output = ((detection_field_value, percentage))
    detection_freq = {}
    total_count = 0
    for line in sys.stdin:
        one, det_tuple = line.split('\t')
        detection, freq = ast.literal_eval(det_tuple)

        total_count += int(freq)

        # detection_freq[detection] = detection_freq.get(detection, 0) + 1

        # reducer one should output each key and freq, so we expect reducer 2 to see only one unique occurrence of each key.
        detection_freq[detection] = int(freq)

    # print the (detection field, freq)
    for d in detection_freq:    
        print('%s\t%s' % (d, detection_freq[d] * 100 / total_count ))

