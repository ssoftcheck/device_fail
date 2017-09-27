termination_point = "h_OCHCTP"
file_location = "C:/"
# choose one of auc,phi,acc,sens,spec,balacc
metric = "balacc"
n_folds = 10

# specify predictor variables
vars = c(
  "BerPreFecAve",
  "BerPreFecMax",
  "BerPreFecMin",
  "ChanOchChromaticDispersionAve",
  "ChanOchChromaticDispersionMax",
  "ChanOchChromaticDispersionMin",
  "ChanOchLBCAve",
  "ChanOchLBCMax",
  "ChanOchLBCMin",
  "ChanOchOptAve",
  "ChanOchOptMax",
  "ChanOchOptMin",
  "PmType",
  "PmdAve",
  "PmdMax",
  "PmdMin",
  "Qave",
  "Qmax",
  "Qmin",
  "SoPmdAve",
  "SoPmdMax",
  "SoPmdMin",
  "alarms"
)
var.formula = as.formula(paste0("fail~",paste(vars,collapse="+")))

library(data.table)
library(ggplot2)
library(xgboost)
library(foreach)
library(parallel)
library(lubridate)
library(doSNOW)
library(iterators)
library(pdp)
library(tcltk)
library(DMwR)
library(ROCR)

phi = function(a,p,cutoff=0.5) {
  po = prediction(p,a)
  perf = performance(po,measure = "phi")
  point = which.min(abs(perf@x.values[[1]]-cutoff))[1]
  return(perf@y.values[[1]][point])
}
auc = function(a,p) {
  po = prediction(p,a)
  perf = performance(po,measure = "auc")
  return(perf@y.values[[1]])
}
accuracy = function(a,p,cutoff=0.5) {
  po = prediction(p,a)
  perf = performance(po,measure = "acc")
  point = which.min(abs(perf@x.values[[1]]-cutoff))[1]
  return(perf@y.values[[1]][point])
}
sensitivity = function(a,p,cutoff=0.5) {
  po = prediction(p,a)
  perf = performance(po,measure = "sens")
  point = which.min(abs(perf@x.values[[1]]-cutoff))[1]
  return(perf@y.values[[1]][point])
}
specificity = function(a,p,cutoff=0.5) {
  po = prediction(p,a)
  perf = performance(po,measure = "spec")
  point = which.min(abs(perf@x.values[[1]]-cutoff))[1]
  return(perf@y.values[[1]][point])
}

sense.xgb = function(preds, dtrain) {
  labels = getinfo(dtrain, "label")
  val = sensitivity(labels,preds,0.5)
  return(list(metric = "sensitivity", value = val))
}

balacc.xgb = function(preds,dtrain) {
  labels = getinfo(dtrain, "label")
  sens = sensitivity(labels,preds,0.5)
  spec = specificity(labels,preds,0.5)
  val = (sens+spec)/2
  return(list(metric = "bal_acc", value = val))
}

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