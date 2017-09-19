"""
@author: Sean Softcheck
REQUIRED: extract_data.py in same directory as this script

Use with an input csv file with these fields
location: location of raw zip files
outdir: location to save data
alarm: location of alarm/event tar.gz files
date: date of failure, e.g. 2017-03-21
node: node name, e.g. HSTSTXJVO541A2008A
fail_point: termination point block where failure ocurred. All sub-components will also be included. e.g. 1-A-3 also includes 1-A-3-L1, 1-A-3-L2 etc
fail_start: optional time at which failure ocurred, e.g. 2017-05-01 23:14:00
fail_end: required if fail_start is supplied, same format

usage: python.exe call_extract_data -i your_csv.csv
"""

import os
import pandas as pd
import argparse

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Data Creation")
    parser.add_argument("-i","--input", help="Input csv",required=True)
    args = parser.parse_args()
    
    infile = pd.read_csv(args.input,)
    
    tracker = 1    
    for _ in infile.itertuples():
        if not pd.isnull(_.fail_point):
            tp = " --targetTerminationPoint " + _.fail_point
        else:
            tp = ""
        if not pd.isnull(_.fail_start):
            f = " --fail \"" + _.fail_start + "\""
        else:
            f = ""
        if not pd.isnull(_.fail_end):
            fin = " --finished \"" + _.fail_end + "\""
        else:
            fin = ""
            
        print("Starting Event " + str(tracker) + " of " + str(infile.shape[0]))
        call = "python extract_data.py" + " --ziploc \"" + _.location + "\" --outdir \"" + _.outdir + "\" --alarmloc \"" + _ .alarm + "\" --targetDate " + _.date + " --targetNode " + _.node + tp + f + fin
        print("\n" + call + "\n")
        os.system(call)
        tracker += 1