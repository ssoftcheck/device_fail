# -*- coding: utf-8 -*-
"""
Created on Fri Aug  4 12:54:10 2017

@author: AB58342
"""
import os
import pandas as pd
import argparse

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Data Creation")
    parser.add_argument("-i","--input", help="Input csv",required=True)
    args = parser.parse_args()
    
    infile = pd.read_csv(args.input)
    
    tracker = 1    
    for _ in infile.itertuples():
        if _.fail_point != "":
            tp = " --targetTerminationPoint " + _.fail_point
        else:
            tp = ""
        if _.fail_start != "":
            f = " --fail " + _.fail_start
        else:
            f = ""
        if _.fail_end != "":
            fin = " --finished " + _.fail_end
        else:
            fin = ""
            
        print("Starting Event " + str(tracker) + " of " + str(infile.shape[0]))
        os.system("python extract_data.py" + " --ziploc \"" + _.location + "\" --targetDate " + _.date + " --targetNode " + _.node + tp + f + fin)
        tracker += 1