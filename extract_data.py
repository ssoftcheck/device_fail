# -*- coding: utf-8 -*-
"""
Created on Thu Aug  3 14:34:42 2017

@author: AB58342
"""

import pandas as pd
import zipfile as zf
import os
import datetime as dt
import re
from tqdm import tqdm
import argparse
from shutil import copyfile

parser = argparse.ArgumentParser(description="Script to exctract data")
parser.add_argument("-l","--ziploc", help="location of zip files",required=True)
parser.add_argument("-o","--outdir",help="directory for output folders",required=False)
parser.add_argument("-d","--targetDate", help="date of failure as numeric %d/%m/%Y",required=True)
parser.add_argument("-n","--targetNode", help="node of failure",required=True)
parser.add_argument("-t","--targetTerminationPoint", help="termination point of failure",required=False)
parser.add_argument("-f","--fail", help="failure datetime as numeric Year.Month.Day.Hour.Minute.Second",required=False)
parser.add_argument("-r","--finished", help="failure resolution datetime as numeric Year.Month.Day.Hour.Minute.Second",required=False)
parser.add_argument("-p","--pickle",const="F",choices=["F","f","T","t"],nargs="?",help="T/F flag to write pickled files also.")
parser.add_argument("-x","--excel",const="F",choices=["F","f","T","t"],nargs="?",help="T/F flag to write excel files also. Default False because it is slow")
args = parser.parse_args()

# create regex for the dates based on the target date
ziploc = args.ziploc 
tempDir = ziploc + "temp/"
if args.outdir is None:
    outdir = re.search(r"(.+/)(.+/$)",ziploc).group(1)
else:
    outdir = args.outdir

targetDate = dt.datetime.strptime(args.targetDate,"%m/%d/%Y") 
targetNode = args.targetNode

if args.targetTerminationPoint is not None:
    targetTerminationPoint = args.targetTerminationPoint
    targetTerminationPoint = r"^" + targetTerminationPoint.replace("-",r"\-")
else:
    targetTerminationPoint = None
    
if args.fail is not None:
    fail = args.fail
    failureTime = dt.datetime.strptime(fail,"%Y.%m.%d.%H.%M.%S")
    if args.finished is not None:
        finishTime = args.finished
        finishTime = dt.datetime.strptime(finishTime,"%Y.%m.%d.%H.%M.%S")
    else:
        finishTime = failureTime
else:
    failureTime = None
    finishTime = None

dates = [targetDate + dt.timedelta(days=x) for x in range(-7,2) if targetDate + dt.timedelta(days=x) >= dt.datetime(2017,1,1)]
# special case for target date between [2017.01.24,2017.02.15]
if any(x >= dt.datetime(2017,1,24) and x <= dt.datetime(2017,2,15) for x in dates):
    dates.append(dt.datetime(2017,2,16))

files = ["^" + str(x.day) + r"\_" + str(x.month) + r"\_" + str(x.year) + r".+zip$" for x in dates]
files = "|".join(files)

# TODO: connect to server and download files into ziploc, or get packages on server to run directly

# find the exact file names for the relevant targetDate
fileList = os.listdir(ziploc)
targetFiles = [y.group(0) for y in [re.search(files,x,re.IGNORECASE) for x in fileList] if y is not None]

# unzip the files
print("Extracting Relevant Data")
if not os.path.exists(tempDir):
    os.mkdir(tempDir)
for tf in targetFiles:
    zip_ref = zf.ZipFile(ziploc + tf, 'r')
    for each in zip_ref.namelist():
        if re.search(r"^" + targetNode + r"\_pm15min\_.*csv$",each) is not None:
            if not os.path.exists(tempDir + each):
                zip_ref.extract(each,tempDir)
    zip_ref.close()

# get 15 min data
csvList = os.listdir(tempDir)

tracker = {}
print("Processing Extracted csv Files")
for item in tqdm(csvList):
    current = tempDir + item
    termination_point = ""
    rows = []
    with open(current,"r") as cf:
        dataStart = False
        dataRead = False
        for line in cf:
            text = re.sub(r"\n$","",line)
            text = text.split(",")
            if text[0] == "NODEID":
                node_id = text[1]
            elif text[0] == "NETYPE":
                netype = text[1]
            elif text[0] == "ISNODECONTROLLER":
                node_ctrl = text[1]
            elif text[0] == "CHASSISTYPE":
                chassis_type = text[1]
            elif text[0] == "SourceChassisId":
                chassis_id = text[1]
            # starting a chunk of data
            if line == "\n" or line == "":
                dataStart = True
                dataRead = False
                # if the dataframe is ready to be written to csv
                if len(rows) > 0:
                    if not os.path.exists(outdir + str(termination_point)):
                        os.mkdir(outdir + str(termination_point))
                    pd.DataFrame(rows).to_csv(outdir + "termination_point/" + str(termination_point) + "/" + item,index=False)
                    if termination_point in tracker:
                        tracker[termination_point].append(outdir + "termination_point/" + str(termination_point) + "/" + item)
                    else:
                        tracker[termination_point] = [outdir + "termination_point/" + str(termination_point) + "/" + item]
                    rows = []
            elif dataStart:
                termination_point = text[0]
                header = ["node_id","chassis_id","chassis_type","netype","node_ctrl","termination_point"] + text[1:]
                # deal with duplciate header names
                headerDict = dict()
                for h in range(len(header)):
                    if header[h] not in headerDict:
                        headerDict[header[h]] = 1
                    else:
                        headerDict[header[h]] = headerDict[header[h]] + 1
                        header[h] = header[h] + "_" + str(headerDict[header[h]]                        )
                df = pd.DataFrame(columns = header)
                dataStart = False
                dataRead = True
            elif dataRead:
                rows.append(dict(zip(header,[node_id,chassis_id,chassis_type,netype,node_ctrl] + text)))


# delete tempDir
print("Removing Extracted Files")
for each in os.listdir(tempDir):
    os.remove(tempDir + each)
os.removedirs(tempDir)

# assemble datasets
print("Combining Processed csv Files by Termination Point")
hDirs = tracker.keys()
for each in tqdm(hDirs):
    for csv in tracker[each]:
        df = pd.read_csv(csv)
        # now remove csv+ 
        os.remove(csv)
        df["timestamp"] = df["timestamp"].apply(lambda x: dt.datetime.strptime(x, "%Y.%m.%d.%H.%M.%S"))
        if "df_all" not in locals():
            df_all = df.copy()
        else:
            df_all = df_all.append(df)
    if len(tracker[each]) > 0:
        df_all = df_all.sort_values(by=["node_id","chassis_id","chassis_type","termination_point","timestamp"])
        if failureTime is not None:
            df_all["fail"] = 0
            df_all["time_lag"] = df_all[["termination_point","timestamp"]].groupby("termination_point").shift(1)
            df_all["time_jump"] = df_all[["termination_point","timestamp"]].groupby("termination_point").shift(-1)
            term_point_ind = df_all["termination_point"].apply(lambda x: re.search(targetTerminationPoint,x) is not None)
            df_all.loc[(term_point_ind) & 
                       ((df_all["timestamp"] >= failureTime) & (df_all["time_lag"] < failureTime) |
                        (df_all["timestamp"] <= finishTime) & (df_all["time_jump"] > finishTime)),"fail"] = 1
            start_end = df_all.loc[df_all["fail"]==1,["termination_point","timestamp"]].groupby("termination_point")
            start_end = start_end.aggregate([min,max])
            for sei in start_end.index:
                df_all.loc[(df_all["termination_point"] == sei) & 
                           (df_all["timestamp"] >= start_end.loc[sei,"timestamp"]["min"]) &
                           (df_all["timestamp"] < start_end.loc[sei,"timestamp"]["max"]),"fail"] = 1
                # add hours until failure
                hours_until = start_end.loc[sei,"timestamp"]["min"] - df_all.loc[(df_all["termination_point"] == sei),"timestamp"]
                hours_until = hours_until.apply(lambda x: x.total_seconds() / 60**2)
                df_all.loc[(df_all["termination_point"] == sei),"hours_until"] = hours_until
            
            del df_all["time_lag"],df_all["time_jump"]
		   # remove duplciates
        df_all = df_all.drop_duplicates()
        # write to csv
        df_all.to_csv(outdir + "termination_point/" + each + each.replace(r"/","")  + "_" + targetNode + "_" + str(targetDate.date()) + ".csv",index=False)
        # copy to special folder for files that have events
        if df_all["fail"].sum() > 0:
            if not os.path.exists(outdir + "failure_files/"):
                os.mkdir(outdir + "failure_files/")
            copyfile(outdir + "termination_point/" + each + each.replace(r"/","")  + "_" + targetNode + "_" + str(targetDate.date()) + ".csv",
                     outdir + "failure_files/" + each.replace(r"/","")  + "_" + targetNode + "_" + str(targetDate.date()) + ".csv")
        # write dataframe as pickle object
        if args.pickle in ["T","t"]:
            df_all.to_pickle(outdir + "termination_point/" + each + each.replace(r"/","")  + "_" + targetNode + "_" + str(targetDate.date()) + ".pickle")        
        if args.excel in ["T","t"]:
            df_all.to_excel(outdir + "termination_point/" + each + each.replace(r"/","")  + ".xlsx",index=False)
        del df_all
