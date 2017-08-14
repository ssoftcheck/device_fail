# -*- coding: utf-8 -*-
"""
Created on Fri Aug  4 12:54:10 2017

@author: AB58342
"""
import os

if __name__ == "__main__":
    dataloc = '"C:/Users/AB58342/Documents/device failures/data/"'
    dates = ["21/3/2017","15/1/2017","28/2/2017","2/3/2017","4/3/2017","1/5/2017"]
    nodes = ["ATLNGAMAO50080604A","ASBNVACZO16C20113A","PTLDORWAO23041506A","FRPKGACXO25014905A","DNVRCO26O70040405A","DNVRCOMAO09072501A"]
    termPoints = ["11-A-5","1-A-3","5-A-4","4-A-3","13-B-1","8-A-3"]
    fail = ["2017.3.21.15.19.00","2017.1.15.9.16.0","2017.2.28.1.35.0","2017.3.2.17.23.0","2017.3.4.5.54.0","2017.5.1.23.14.0"]
    finished = ["" for i in range(len(fail))]
	
    for _ in range(len(dates)):
        if termPoints[_] != "":
            tp = " --targetTerminationPoint " + termPoints[_]
        else:
            tp = ""
        if fail[_] != "":
            f = " --fail " + fail[_]
        else:
            f = ""
        if finished[_] != "":
            fin = " --finished " + finished[_]
        else:
            fin = ""
        print("Starting Event " + str(_+1) + " of " + str(len(dates)))
        os.system("python extract_data.py" + " --ziploc " + dataloc + " --targetDate " + dates[_] + " --targetNode " + nodes[_] + tp + f + fin)