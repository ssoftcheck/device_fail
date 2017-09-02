# -*- coding: utf-8 -*-
"""
Created on Tue Aug 29 20:14:12 2017

@author: ab39138
"""

import tarfile
import re
import os
import pandas as pd
import numpy as np
import datetime as dt
import shutil

# round to nearest integer multiple of m (15 minutes default)
def roundUp(n, m=15):
    adjust = n.minute % m 
    change = (m - adjust) if adjust > 0 else 0
    return n + dt.timedelta(minutes=change,seconds=-n.second)

def roundNearest(n, m=15):
    adjust = n.minute % m 
    change = -adjust if adjust < m/2 else m - adjust
    return n + dt.timedelta(minutes=change,seconds=-n.second)
  
alarm_dates = [targetDate - dt.timedelta(days=33),targetDate + dt.timedelta(days=33)]
file_names = [x for x in os.listdir(alarmloc) if x.endswith(".tar.gz")]
file_dates = [dt.datetime.strptime(x[:11],"%b_%d_%Y") for x in file_names]
file_names = [x for x,y in zip(file_names,file_dates) if  y >= dates[0]] and y <= dates[1]]

byvars = ["node_id","termination_point","merge_time_up","merge_time_nearest"]
col_order = ["Type", "ID", "Notification ID", "Severity", "Node ID", "Node Name",
             "Node Label", "Source Object", "Object Type", "Event Type",
             "Event Sub Type", "EMS Received Time", "Alarm Correlation ID",
             "Source Date/Time", "FaultConditions", "Message", "Changed Variables",
             "Circuit ID", "Additional Text", "Customer Name(s)","FaultConditionsInHex"]
print("Extracting Alarm Data")
for fn in tqdm(file_names):
    tar = tarfile.open(alarmloc + fn)
    tar.extractall(alarmloc + "temp_extract/")
    tar.close()
    
    # Data starts on row 10
    current = re.search(r"(.+).tar.gz",fn).group(1)
    elog = pd.read_csv(alarmloc + "temp_extract/" + current,sep="\t",skiprows=9,index_col=False)
    elog = elog.loc[elog["Event Sub Type"] == "ALARM"]
    	
    #elog = pd.read_csv(path + fname, sep='\t', skiprows=9)
    elog.columns = list(map(lambda x: x.replace("#",""),elog.columns.values))
    elog = elog.loc[:,col_order]
    elog["Source Date/Time"] = elog["Source Date/Time"].apply(lambda x: dt.datetime.strptime(x[:19],"%Y-%m-%d %H:%M:%S"))
    elog.rename(columns={"Source Object":"termination_point"},inplace=True)
    elog["node_id"] = elog["Node Name"] + "@" + elog["Node ID"]
    elog.drop(["Node Name","Node ID","Node Label"],axis=1,inplace=True)
     
    elog["merge_time_up"] = elog["Source Date/Time"].apply(roundUp)
    elog["merge_time_nearest"] = elog["Source Date/Time"].apply(roundNearest)
    
    if "elog_all" not in locals():
        elog_all = elog.copy()
    else:
        elog_all = pd.concat([elog_all,elog])
    del elog

elog_all = elog_all.loc[byvars]
elog_all = pd.concat([elog_all,pd.DataFrame({"n":np.repeat(1,elog_all.shape[0])})],axis=1).groupby(byvars).count().reset_index()
shutil.rmtree(alarmloc + "temp_extract/")