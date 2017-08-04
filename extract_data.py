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

# create regex for the dates based on the target date
targetDate = dt.datetime.strptime("21/3/2017","%d/%m/%Y")
targetNode = "ATLNGAMAO50080604A"
ziploc = "C:/Users/AB58342/Documents/device failures/data/"
tempDir = ziploc + "temp/"

dates = [targetDate + dt.timedelta(days=x) for x in range(-7,2)]
files = [str(x.day) + r"\_" + str(x.month) + r"\_" + str(x.year) + r".+zip" for x in dates ]
files = "|".join(files)

# TODO: connect to server and download files into ziploc, or get packages on server to run directly


# find the exact file names for the relevant targetDate
fileList = os.listdir(ziploc)
targetFiles = [x.group(0) for x in [re.search(files,x,re.IGNORECASE)  for x in fileList] if x is not None]

# unzip the files
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

for item in tqdm(csvList):
    current = tempDir + item
    component = ""
    rows = []
    with open(current,"r") as cf:
        dataStart = False
        dataRead = False
        for line in cf:
            text = re.sub(r"\n$","",line)
            text = text.split(",")
            if text[0] == "NODEID":
                nodeid = text[1]
            if text[0] == "CHASSISTYPE":
                chassistype = text[1]
            if text[0] == "SourceChassisId":
                chassisID = text[1]
            # starting a chunk of data
            if line == "\n" or line == "":
                dataStart = True
                dataRead = False
                # if the dataframe is ready to be written to csv
                if len(rows) > 0:
                    if not os.path.exists(ziploc + str(component)):
                        os.mkdir(ziploc + str(component))
                    pd.DataFrame(rows).to_csv(ziploc + str(component) + "/" + item,index=False)
                    rows = []
            elif dataStart:
                component = text[0]
                header = ["nodeID","chassisID","chassistype","component"] + text[1:]
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
                rows.append(dict(zip(header,[nodeid,chassisID,chassistype] + text)))


# delete tempDir
for each in os.listdir(tempDir):
    os.remove(tempDir + each)
os.removedirs(tempDir)

# assemble datasets
# TODO: failure = datetime here to create event column
hDirs = [x.group(0) + "/" for x in [re.search(r"^h\_.+",x,re.IGNORECASE) for x in os.listdir(ziploc)] if x is not None]
for each in tqdm(hDirs):
    csv = [x.group(0) for x in [re.search(r".+\.csv$",x,re.IGNORECASE) for x in os.listdir(ziploc + each)] if x is not None]
    for _csv_ in csv:
        df = pd.read_csv(ziploc + each + _csv_)
        df["timestamp"] = df["timestamp"].apply(lambda x: dt.datetime.strptime(x, "%Y.%m.%d.%H.%M.%S"))
        # TODO: add creation of failure column
        if "df_all" not in locals():
            df_all = df.copy()
        else:
            df_all = df_all.append(df)
    # write dataframe as pickle object
    df_all.to_pickle(ziploc + each + each.replace(r"/","")  + ".pickle")
    # xlsx is slow and not really needed, eventually delete or comment out
    df_all.to_excel(ziploc + each + each.replace(r"/","")  + ".xlsx",index=False)
    del df_all






