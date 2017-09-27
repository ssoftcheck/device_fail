termination_point = "h_OCHCTP"
file_location = "C:/"

library(foreach)
library(doSNOW)
library(parallel)
library(ecp)
library(data.table)
library(lubridate)
library(ggplot2)
library(progress)
library(tcltk)
library(iterators)

readData = function(x,filterTime=-1) {
  # filter days to save time/space
  d=fread(x)
  d[,timestamp:=ymd_hms(timestamp)]
  d = d[order(termination_point,timestamp)]
  if(filterTime > 0) {
    failtime = min(d[fail==1,timestamp])
    if(!is.na(failtime))
      d = d[difftime(failtime,timestamp,units="hours") < filterTime]
  }
  return(d)
}