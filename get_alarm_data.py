# -*- coding: utf-8 -*-
"""
Created on Tue Aug 29 20:14:12 2017

@author: ab39138
"""

import tarfile
import sys
import argparse
import re
import os
import pandas as pd
from pandas import DataFrame
import numpy as np
import datetime as dt
  
path = "D:/AUGUST_2_2017_INFINERA/AUG_28_2017_INFINERA/again3/test/"
dates = ["%m_%d_%Y"].join("|")
filenames = [x.group(0) for x in [re.search(dates,y,re.IGNORECASE) for y in os.listdir(path)] if x is not None]

#This is useful mainly when we are merging all "event+log" files, but we cannot based on RAM limitations, so this may not be required
for fn in filenames:
    # gunzip the gz then untar
    tar = tarfile.open(path + fn,"r")
	fname = tar.list
	tar.extractall(path + "temp_extract/")
    tar.close()

    # Data starts on row 10
	elog = pd.read_csv("Mar_15_2017_00-00-01_events.tsv",sep="\t",skiprows=9,index_col=False)
	
    #elog = pd.read_csv(path + fname, sep='\t', skiprows=9)
	elog.columns = list(map(lambda x: x.replace("#",""),elog.columns.values))
	col_order = ["Type", "ID", "Notification ID", "Severity", "Node ID", "Node Name",
 "Node Label", "Source Object", "Object Type", "Event Type",
 "Event Sub Type", "EMS Received Time", "Alarm Correlation ID",
 "Source Date/Time", "FaultConditions", "Message", "Changed Variables",
 "Circuit ID", "Additional Text", "Customer Name(s)",
 "FaultConditionsInHex"]
    elog = elog.loc[:,col_order]
    elog["Source Date/Time"] = elog["Source Date/Time"].apply(lambda x: dt.datetime.strptime(x[:19],"%Y-%m-%d %H:%M:%S"))
    elog.rename(columns={"Source Object":"termination_point"},inplace=True)
    elog["node_id"] = elog["Node Name"] + "@" + elog["Node ID"]
    elog.drop(["Node Name","Node ID","Node Label"],axis=1,inplace=True)

#details for every failure node / timsestamp / NE
nodes = ["ATLNGAMAO50080604A"]
termPoints = ["11-A-5"]
filter
fail = "2017.3.21.15.19.00"
tm = 15.19


# round to nearest integer multiple of m (15 minutes default)
def roundUp(n, m=15):
    adjust = n.minute % m 
    change = (m - adjust) if adjust > 0 else 0
    return n + dt.timedelta(minutes=change,seconds=-n.second)

def roundNearest(n, m=15):
    adjust = n.minute % m 
    change = -adjust if adjust < m/2 else m - adjust
    return n + dt.timedelta(minutes=change,seconds=-n.second) 
 
elog["merge_time_up"] = elog["Source Date/Time"].apply(roundUp)
elog["merge_time_nearest"] = elog["Source Date/Time"].apply(roundNearest)

byvars = ["node_id","termination_point","merge_time_up","merge_time_nearest"]
elog_merge = pd.concat([elog[byvars],pd.DataFrame({"n":np.repeat(1,elog.shape[0])})],axis=1).groupby(byvars).count().reset_index()

__base__.merge(elog_merge,how="left",left_on=["node_id","termination_point","timestamp"],
right_on=["node_id","termination_point","merge_time_up"])


