#!/usr/bin/python3

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
#                                                                                                           #
#  This script will anonymize ZOLL and NIRS patient data.                                                   #
#                                                                                                           #
#  - All timestamps will be obfuscated by shifting them to distant past, while maintaining data integrity.  #
#  - ZOLL startup time (DeviceConfiguration => DevDateTime) will be used as an anchor point for time shift. #
#  - Temporal alignment between ZOLL and NIRS data will be preserved.                                       #
#  - Original timestamps cannot be recovored from output data.                                              #
#  - Patient age field will be cleared.                                                                     #
#                                                                                                           #
#  Validation: - ZOLL data can still be opened in the RescueNet Code Review program after processing.       #
#              - No 21st century dates can be found by searching the output files.                          #
#                                                                                                           #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

import os, sys
import argparse
import json
from datetime import datetime

# ZOLL startup time will be shifted to this point. All other timestamps will be shifted by the same amount of time.
time_zero = '1990-01-01T00:00:00'

format_string = '%Y-%m-%dT%H:%M:%S' # ISO 8601 time format is used by ZOLL.

t_zero = datetime.strptime(time_zero, format_string)

arg_parser = argparse.ArgumentParser(description = "Anonymize patient data. Output files will have the suffix '_anon'.")
arg_parser.add_argument("zoll_json_file")
arg_parser.add_argument("nirs_csv_file")
arguments = arg_parser.parse_args()

zoll_file = arguments.zoll_json_file
nirs_file = arguments.nirs_csv_file

# read input files
try:
    with open(zoll_file, 'r') as fd:
        print("Loading ZOLL file {f}".format(f=zoll_file))
        zoll_data = json.load(fd)
except OSError:
    print("Could not open/read file:", zoll_file)
    sys.exit()

try:
    with open(nirs_file, 'r') as fd:
        print("Loading NIRS file {f}".format(f=nirs_file))
        nirs_data = fd.readlines()
except OSError:
    print("Could not open/read file:", nirs_file)
    sys.exit()

def append_suffix(filename, suffix):
    name, ext = os.path.splitext(filename)
    return "{name}_{sfx}{ext}".format(name=name, sfx=suffix, ext=ext)

def item_generator(json_input, lookup_key):
    if isinstance(json_input, dict):
        for k, v in json_input.items():
            if k == lookup_key:
                yield v
            else:
                yield from item_generator(v, lookup_key)
    elif isinstance(json_input, list):
        for item in json_input:
            yield from item_generator(item, lookup_key)

for item in item_generator(zoll_data, 'DeviceConfiguration'):
    zoll_config = item

zoll_starttime = datetime.strptime(zoll_config['StdHdr']['DevDateTime'], format_string)
timeoffset = zoll_starttime - t_zero

def anonymize_zoll(json_input, offset):
    global n_modified
    timestamp_names = ['DevDateTime', 'StartTime', 'LastTreatmentTimeStamp']
    if isinstance(json_input, dict):
        for k, v in json_input.items():
            if k in timestamp_names:
                # shift timestamp
                newtime = datetime.strptime(v, format_string) - offset
                json_input[k] = newtime.strftime(format_string)
                n_modified += 1
            elif k == 'Age':
                # clear patient age
                json_input[k] = 0
                print("Patient age field cleared")
            else:
                anonymize_zoll(v, offset)
    elif isinstance(json_input, list):
        for item in json_input:
            anonymize_zoll(item, offset)

def anonymize_nirs(nirs_lines, offset):
    global n_modified
    # parse start timestamp
    nirs_startdate = nirs_lines[1].partition(',')[2].strip()
    nirs_starttime = nirs_lines[6].partition(',')[0].strip()
    nirs_start = datetime.strptime(nirs_startdate + 'T' + nirs_starttime, format_string)
    nirs_newstartdate = (nirs_start - offset).strftime("%Y-%m-%d")
    nirs_lines[1] = nirs_lines[1].split(',')[0] + ',' + nirs_newstartdate + '\n'
    nirs_date = nirs_startdate
    prev = None
    for i in range(len(nirs_lines)):
        if i < 6: continue
        t = nirs_lines[i].partition(',')[0]
        dt = datetime.strptime(nirs_date + 'T' + t, format_string)
        if prev is not None and dt < prev:  # handle midnight rollover
            dt += datetime.timedelta(days=1)
            nirs_date = dt.strftime("%Y-%m-%d")
        prev = dt
        shifted_dt = dt - offset
        nirs_lines[i] = shifted_dt.strftime("%H:%M:%S") + ',' + nirs_lines[i].split(',', 1)[1]
        n_modified += 1

print("Processing ZOLL data")
n_modified = 0
anonymize_zoll(zoll_data, timeoffset)
print("{n} timestamps modified.".format(n=n_modified))

print("Processing NIRS data")
n_modified = 0
anonymize_nirs(nirs_data, timeoffset)
print("{n} timestamps modified.".format(n=n_modified))

zoll_outfile = append_suffix(zoll_file, 'anon')
nirs_outfile = append_suffix(nirs_file, 'anon')

# write output files
try:
    with open(zoll_outfile, 'w') as fd:
        print("Writing ZOLL output to {f}".format(f=zoll_outfile))
        json.dump(zoll_data, fd, ensure_ascii=False, separators=(',', ':'))
except OSError:
    print("Could not open/write file:", zoll_outfile)
    sys.exit()

try:
    with open(nirs_outfile, 'w') as fd:
        print("Writing NIRS output to {f}".format(f=nirs_outfile))
        fd.writelines(nirs_data)
except OSError:
    print("Could not open/write file:", nirs_outfile)
    sys.exit()
