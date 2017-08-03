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

# create regex for the dates based on the target date
targetDate = dt.datetime.strptime("21/3/2017","%d/%m/%Y")
dates = [targetDate + dt.timedelta(days=x) for x in range(-7,2)]
files = [str(x.day) + r"\_" + str(x.month) + r"\_" + str(x.year) + r".+zip" for x in dates ]
files = "|".join(files)

# TODO: connect to server and download files into ziploc

ziploc = "C:/Users/AB58342/Documents/device failures/data/"

# find the exact file names for the relevant targetDate
fileList = os.listdir(ziploc)
targetFiles = [re.findall(files,x,re.IGNORECASE)  for x in fileList]
targetFiles = [x for s in targetFiles for x in s]
targetFiles


# unzip the files
tempDir = ziploc + "temp/"
if not os.path.exists(tempDir):
    os.mkdir(tempDir)
for tf in [targetFiles[1]]:
    zip_ref = zf.ZipFile(ziploc + tf, 'r')
    zip_ref.extractall(tempDir)
    zip_ref.close()

# get 15 min data
csvList = [re.findall(r".+\_pm15min\_.+csv$",x,re.IGNORECASE) for x in os.listdir(tempDir)]
csvList = [x for s in csvList for x in s]
csvList

# TODO: start reading files into dataframes