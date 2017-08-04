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

parser = argparse.ArgumentParser(description="Script to exctract data")
parser.add_argument("-l","--ziploc", help="location of zip files",required=True)
parser.add_argument("-d","--targetDate", help="date of failure as numeric %d/%m/%Y",required=True)
parser.add_argument("-n","--targetNode", help="node of failure",required=True)
parser.add_argument("-t","--targetTerminationPoint", help="termination point of failure",required=False)
parser.add_argument("-f","--fail", help="failure datetime as numeric Year.Month.Day.Hour.Minute.Second",required=False)
parser.add_argument("-x","--excel",const="F",choices=["F","f","T","t"],nargs="?",help="T/F flag to write excel files also. Default False because it is slow")
args = parser.parse_args()

# create regex for the dates based on the target date
ziploc = args.ziploc # "C:/Users/AB58342/Documents/device failures/data/"
tempDir = ziploc + "temp/"

targetDate = dt.datetime.strptime(args.targetDate,"%d/%m/%Y") # dt.datetime.strptime("21/3/2017","%d/%m/%Y")
targetNode = args.targetNode # "ATLNGAMAO50080604A"
if args.targetTerminationPoint is not None:
    targetTerminationPoint = args.targetTerminationPoint # "10-A-1"
    targetTerminationPoint = r"^" + targetTerminationPoint.replace("-",r"\-")
else:
    targetTerminationPoint = None
if args.fail is not None:
    fail = args.fail # "2017.03.13.12.35.00"
    failureTime = dt.datetime.strptime(fail,"%Y.%m.%d.%H.%M.%S")
else:
    failureTime = None

dates = [targetDate + dt.timedelta(days=x) for x in range(-7,2)]
files = [str(x.day) + r"\_" + str(x.month) + r"\_" + str(x.year) + r".+zip" for x in dates]
files = "|".join(files)

# TODO: connect to server and download files into ziploc, or get packages on server to run directly

# find the exact file names for the relevant targetDate
fileList = os.listdir(ziploc)
targetFiles = [x.group(0) for x in [re.search(files,x,re.IGNORECASE)  for x in fileList] if x is not None]

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
            if text[0] == "CHASSISTYPE":
                chassis_type = text[1]
            if text[0] == "SourceChassisId":
                chassis_id = text[1]
            # starting a chunk of data
            if line == "\n" or line == "":
                dataStart = True
                dataRead = False
                # if the dataframe is ready to be written to csv
                if len(rows) > 0:
                    if not os.path.exists(ziploc + str(termination_point)):
                        os.mkdir(ziploc + str(termination_point))
                    pd.DataFrame(rows).to_csv(ziploc + str(termination_point) + "/" + item,index=False)
                    rows = []
            elif dataStart:
                termination_point = text[0]
                header = ["node_id","chassis_id","chassis_type","termination_point"] + text[1:]
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
                rows.append(dict(zip(header,[node_id,chassis_id,chassis_type] + text)))


# delete tempDir
print("Removing Extracted Files")
for each in os.listdir(tempDir):
    os.remove(tempDir + each)
os.removedirs(tempDir)

# assemble datasets
print("Combining Processed csv Files by Termination Point")
hDirs = [x.group(0) + "/" for x in [re.search(r"^h\_.+",x,re.IGNORECASE) for x in os.listdir(ziploc)] if x is not None]
for each in tqdm(hDirs):
    csv = [x.group(0) for x in [re.search(r".+\.csv$",x,re.IGNORECASE) for x in os.listdir(ziploc + each)] if x is not None]
    csv = [x for x in csv if not x.startswith("h_")]
    for _csv_ in csv:
        df = pd.read_csv(ziploc + each + _csv_)
        # now remove csv
        os.remove(ziploc + each + _csv_)
        df["timestamp"] = df["timestamp"].apply(lambda x: dt.datetime.strptime(x, "%Y.%m.%d.%H.%M.%S"))
        if "df_all" not in locals():
            df_all = df.copy()
        else:
            df_all = df_all.append(df)
    if len(csv) > 0:
        df_all = df_all.sort_values(by=["node_id","chassis_id","chassis_type","termination_point","timestamp"])
        # TODO: add creation of failure column
        if failureTime is not None:
            df_all["fail"] = 0
            df_all["time_lag"] = df_all[["termination_point","timestamp"]].groupby("termination_point").shift(1)
            df_all.loc[(df_all["termination_point"].apply(lambda x: re.search(targetTerminationPoint,x) is not None)) & 
                       (df_all["timestamp"] >= failureTime) & (df_all["time_lag"] < failureTime),"fail"] = 1
        # write dataframe as pickle object
        df_all.to_pickle(ziploc + each + each.replace(r"/","")  + "_" + targetNode + "_" + str(targetDate.date()) + ".pickle")
        df_all.to_csv(ziploc + each + each.replace(r"/","")  + "_" + targetNode + "_" + str(targetDate.date()) + ".csv",index=False)
        if args.excel in ["T","t"]:
            df_all.to_excel(ziploc + each + each.replace(r"/","")  + ".xlsx",index=False)
        del df_all