# -*- coding: utf-8 -*-
"""
Created on Tue Aug 29 20:14:12 2017

@author: ab39138
"""


import tarfile,sys
import argparse
import re,os
import pandas as pd
from pandas import DataFrame
import numpy as np
  
path = "D:/AUGUST_2_2017_INFINERA/AUG_28_2017_INFINERA/again3/test/"
#file = "D:/AUGUST_2_2017_INFINERA/AUG_28_2017_INFINERA/Feb_01_2017_00-00-01_events.tsv"
#fname = "Apr_01_2017_00-00-01_events.csv"

header = pd.read_csv("D:/AUGUST_2_2017_INFINERA/AUG_28_2017_INFINERA/header.csv")
os.chdir(path)

#parser = argparse.ArgumentParser(description="Script to extract data")

#This is useful mainly when we are merging all "event+log" files, but we cannot based on RAM limitations, so this may not be required
for filename in os.listdir(path):
    #code to create "tsv" file
    tar = tarfile.open(path + filename)
    tar.extractall()
    tar.close()
    #replace "csv" with "tsv" to read the "tsv" file
    end = filename.find('.tsv')
    start = 0
    fname = filename[start:end] + ".tsv"

    #reading the "Tsv" file created above
    #getting errro on row 10, hence need to skip row = 9 and read only data without header.Add header separately 
    elog_orig = pd.read_csv(path + fname, sep='\t', skiprows=9,index_col = False)
    column_order = header.columns
    #concatenate "header row" with "data" 
    frames = [elog_orig,header]
    elog_header = pd.concat([elog_orig,header])
    elog_head_ord = elog_header.ix[:, column_order]
    elog = elog_head_ord
    #sample = file_correct_order.ix[1:100,]
    #incase need to save tsvs as csvs, may be infinera wants these csv extrcted from tar files
    #csvname = re.sub('tsv','csv',fname)
    #file_correct_order.to_csv(path + csvname)  

    
##Second part    
#WE CAN FIRST CONVERT ALL TSV to CSVs and then READ THOSE CSVs
#LOGIC: round to nearest integer multiple of 15
def round_to_nearest(n, m):
    return m if n <= m else round(int(n)/int(m)) * int(m)  

def corrected(m,h):
    if m == 60 :
        h = h + 1
        return h
    else :
        return h

def corrected2(m,h):
    if m == 60 :
       m = 00
       return m
    else :
       return m

#ELOG IS EVENT LOG   
#WE CAN FIRST CONVERT ALL TSV to CSVs and then READ THOSE CSVs
elog = pd.read_csv(path + "Apr_01_2017_00-00-01_events.csv")
#we can read "tsv" file also
elog['datetime'] = pd.to_datetime(elog['Source Date/Time'])
elog['newdate'] = elog['datetime'].map(lambda x: x.strftime('%m/%d/%Y'))
 
#LOGIC 1: round to nearest divisible of 15   
elog['hourr'] = elog['Source Date/Time'].str[11:13].astype(int)
elog['minn'] = elog['Source Date/Time'].str[14:16].astype(int)
elog['min2'] = elog.apply(lambda row: round_to_nearest(row['minn'], 15), axis=1).astype(int)

#LOGIC2: iF minutes values lk (59) is round off to 60, then we neeed to make min as 00 and increment hour by 1
elog['correct_min'] = elog.apply(lambda row: corrected2(row['min2'], row['hourr']), axis=1)
elog['correct_hr'] = elog.apply(lambda row: corrected(row['min2'], row['hourr']), axis=1)

#create new column by combning hour.minutes
elog['new_time'] = elog.correct_hr.astype(str).str.cat(elog.correct_min.astype(str),sep=".")

#elog2 = elog.ix[1:100,]

#details for every failure node / timsestamp / NE
nodes = "ATLNGAMAO50080604A"
termPoints = "11-A-5"
fail = "2017.3.21.15.19.00"
tm = 15.19

#apply filter one by one just to check the records for now, will improve it later
elog2 = elog.loc[(elog['Node Name']== nodes)]

elog3 = elog2[(elog2['newdate'].astype(str) >= "03/06/2017")  & (elog2['new_time'].astype(float) > tm)]
elog4  = elog3.loc[(elog2['Source Object'].astype(str)== termPoints) & (elog3['newdate'].astype(str) <= "01/12/2017")]
#elog4  = elog3.loc[re.match(r'^11-A-5', elog3['Source Object'].astype(str)) ]
pos = fname.find('.csv')
fname2 = fname[0:27] + "_alarm" + ".csv"
elog4.to_csv(path + fname2)


