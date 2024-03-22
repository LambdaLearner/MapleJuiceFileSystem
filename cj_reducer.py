#!/usr/bin/python3
import sys
import re
import ast

if __name__ == "__main__":
    # since reducer takes every (key, row) we are guaranteed that every row has the same value
    # join_col = argv[1]
    data1_list = []
    data2_list = []
    d = {}
    
    # glob_key = ""
    for line in sys.stdin:
        line = line.split('\t')

        key, dr = line[0], line[1]
        data_marker, row = ast.literal_eval(dr)
        
        if d.get(key, {}) == {}:
            d[key]= {}
            d[key]["data1_list"] = []
            d[key]["data2_list"] = []
    
        
        row = ",".join(row)
        
        if data_marker == "d1":
            d[key]["data1_list"].append(row)
        elif data_marker == "d2":
            d[key]["data2_list"].append(row)
        
    for k in d.keys():
        for row1 in d[k]["data1_list"]:
            for row2 in d[k]["data2_list"]:
                print('%s' % (str(row1.strip()) + "," + str(row2.strip())))
