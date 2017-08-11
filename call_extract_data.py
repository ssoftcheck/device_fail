# -*- coding: utf-8 -*-
"""
Created on Fri Aug  4 12:54:10 2017

@author: AB58342
"""
import os

if __name__ == "__main__":
    dataloc = '"C:/Users/AB58342/Documents/device failures/data/"'
    dates = ["21/3/2017"]
    nodes = ["ATLNGAMAO50080604A"]
    termPoints = ["11-A-5"]
    fail = ["2017.3.21.21.18.26"]
    
    for _ in range(len(dates)):
        if termPoints[_] != "":
            tp = " --targetTerminationPoint " + termPoints[_]
        else:
            tp = ""
        if fail[_] != "":
            f = " --fail " + fail[_]
        else:
            f = ""
        print("Starting Event " + str(_+1) + " of " + str(len(dates)))
        os.system("python extract_data.py" + " --ziploc " + dataloc + " --targetDate " + dates[_] + " --targetNode " + nodes[_] + tp + f)
    