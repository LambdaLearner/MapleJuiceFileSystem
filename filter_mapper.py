#!/usr/bin/python3
import sys
import re

if __name__ == "__main__":
    # Return (1, (detection_field_value, frequency))
    regex_str = sys.argv[1]
    for line in sys.stdin:
        row = line.strip().split(',')
        # word_keys = {}
        ans = re.search(regex_str, line) is not None
        if ans:
            # row_id = str(row[0])
            # intermediate_filename = f'/home/sb82/mp4_82/maple_files/{task_dict["intermediate_file_name"]}_{row_id}.maple'
            # file_keys[f'{task_dict["intermediate_file_name"]}_{row_id}.maple'] = 1
            # word_keys[line] = 1
            print('%s\t%s' % (1, line.strip()))
            