#!/usr/bin/python3
import sys
import re

# orig dataset, join dataset, col1, col2
if __name__ == "__main__":
    
    d1_cols = ['ID', 'Source', 'Severity', 'Start_Time', 'End_Time', 'Start_Lat',
       'Start_Lng', 'End_Lat', 'End_Lng', 'Distance(mi)', 'Description',
       'Street', 'City', 'County', 'State', 'Zipcode', 'Country', 'Timezone',
       'Airport_Code', 'Weather_Timestamp', 'Temperature(F)', 'Wind_Chill(F)',
       'Humidity(%)', 'Pressure(in)', 'Visibility(mi)', 'Wind_Direction',
       'Wind_Speed(mph)', 'Precipitation(in)', 'Weather_Condition', 'Amenity',
       'Bump', 'Crossing', 'Give_Way', 'Junction', 'No_Exit', 'Railway',
       'Roundabout', 'Station', 'Stop', 'Traffic_Calming', 'Traffic_Signal',
       'Turning_Loop', 'Sunrise_Sunset', 'Civil_Twilight', 'Nautical_Twilight',
       'Astronomical_Twilight']
    
    d2_cols = ['Weather', 'Weather_bin', "Channel"]

    d1_len, d2_len = len(d1_cols), len(d2_cols)
    
    col1, col2 = "Weather_Condition", "Weather"
    col3, col4 = "Source", "Channel"
    
    #join on Weather from d2 to Weather_Condition on d1

    idx1 = d1_cols.index(col1)
    idx2 = d2_cols.index(col2)

    idx3 = d1_cols.index(col3)
    idx4 = d2_cols.index(col4)

    for line in sys.stdin:
        line = line.strip().split(',')
        if len(line) == d1_len:
            print('%s\t%s' % ((line[idx1], line[idx3]), ("d1", line)))
        elif len(line) == d2_len:
            print('%s\t%s' % ((line[idx2],line[idx4]), ("d2", line)))

