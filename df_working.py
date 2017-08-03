# -*- coding: utf-8 -*-
"""
Created on Mon Jul 24 22:43:26 2017

@author: ab39138
"""
#######removing records after event occured

#https://www.digitalocean.com/community/tutorials/how-to-use-break-continue-and-pass-statements-when-working-with-loops-in-python-3
import pandas as pd
#df = pd.read_csv("C:\\Users\\ab39138\\Desktop\\infinera\\PM_data\\node_files\\CHCGILDTO76072903A.csv",header=None)
df = pd.read_csv("C:\\Users\\ab39138\\Desktop\\infinera\\PM_data\\node_files\\CHRON15MIN3_pm15min_20170314.171005_1.csv",header=None)
#df = pd.read_csv("C:\\Users\\ab39138\\Desktop\\infinera\\PM_data\\march14_march21_final\\june_14\\folder2\\ATLNGAMAO50080604A_pm15min_20170314.171005_5.csv")

i=1
post_rows = 12
length = len(df) -1 
d = {}
df_fin=pd.DataFrame()
number = 4

for number in range(16):
   number = number + 1
   #if number == 5:
    #  continue    # continue here

   #print('Number is ' + str(number))
   post_rows
   df2 = df.loc[post_rows:] # getting all rows for column 2
   df2
   df2['int'] = df2.iloc[:,1]
   df2['int']
#   if df2.int.isnull():
 #      print("break1")    
   first_digit = (df2.int.values == '15').argmax()
   first_digit#23, returns newer index
   first_loc = first_digit + post_rows -1
   first_loc #34 #79
   df3 = df2.loc[first_loc:,]
   df3['int3'] = df2['int'] #getting all rows post row 22
   df3['int3']
   first_null = (df3.int3.isnull()).argmax()
   first_null #67 #76
   len(df3.int3)
   #add blank row
   last_valid = df3.last_valid_index()
   last_valid
   if (first_null + len(df3.int3) - 1) == length:
       print("last set")
       first_null = last_valid +1 
   d[number] = pd.DataFrame()
   d[number] = df2.loc[first_loc :first_null-1,]
   d[number] 
   post_rows = first_null 
   print('Number is ' + str(number))
   #len_chck = first_null + len(df3.int3)
   if first_null > last_valid:
      print("break")
       
print('Out of loop')
#d[5]
#df_fin = d[number]

frames = [d[1],d[2],d[3],d[4],d[5],d[6],d[7]]
df_fin = pd.concat(frames)
#d[1]
#,d[6],d[7],d[8],d[9],d[10],d[11],d[12],d[13],d[14],d[15],d[16]]
df_fin.to_csv("C:\\Users\\ab39138\\Desktop\\infinera\\PM_data\\output\\filechg_5.csv")
    
#last_pos = first_null + len(df3.int3)
    #last_pos
    