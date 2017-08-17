library(foreach)
library(doParallel)
library(ecp)
library(bcp)
library(data.table)
library(lubridate)
library(ggplot2)
library(progress)

setwd("C:/Users/AB58342/Documents/device failures/data/failure_files")
file.names = grep("^h\\_.+csv$",list.files(),value=TRUE)
term.point = regexpr("^h\\_[A-Z]+",file.names,perl=TRUE)
term.point = regmatches(file.names,term.point)

file.names = split(file.names,term.point)

files = lapply(file.names,function(x) Reduce(function(a,b) rbind(a,b,fill=TRUE),Map(fread,x))[order(node_id,termination_point,timestamp)])

for(i in seq(length(files))) {
  files[[i]][,timestamp := ymd_hms(timestamp)]
  # filter days to save time/space
  failtime = min(files[[i]][fail==1,timestamp])
  files[[i]] = files[[i]][difftime(failtime,timestamp,units="hours") < 48]
  rm(failtime)
}

varcheck = Map(function(x) melt(x[,lapply(.SD,function(z) c(miss=sum(is.na(z)),nomiss=sum(!is.na(z)))),by=node_id],id.vars="node_id"),files)
varcheck = Map(function(x) x[,.(miss=value[1],nomiss=value[2]),by=.(node_id,variable)],varcheck)
varcheck = Map(function(x) x[nomiss > 0],varcheck)
for(i in names(varcheck)) {
  varcheck[[i]][,h := i]
}
varcheck = Reduce(rbind,varcheck)
fwrite(varcheck,"variable_sets.csv")


pbar = progress_bar$new(total=length(files),format="[:bar] :current :percent :elapsed")
cl = makeCluster(2)
registerDoParallel(cl)

for(i in names(files)) {
  segments = unique(files[[i]][,.(node_id,termination_point)])
  
  tests = foreach(x = iter(segments,by="row"),.packages=c("data.table","ecp"),.combine="list",.multicombine = TRUE) %dopar% {
    selection = with(files[[i]],node_id==x$node_id & termination_point==x$termination_point)
    vlist = setdiff(names(files[[i]])[sapply(files[[i]][selection],class) %in% c("numeric","integer")],c("timestamp","fail","validity"))
    v = sapply(files[[i]][selection,vlist,with=FALSE],var)
    vlist = vlist[v > 0 & !is.na(v)]
    result = e.divisive(as.matrix(files[[i]][selection,vlist,with=FALSE]),sig.lvl=0.005,R=200,alpha=1)
    result$vars = vlist
    result$name = paste(x,collapse = " ")
    return(result)
  }

  names(tests) = sapply(tests,function(x) x$name)
  
  plots = foreach(x = iter(segments,by="row"),.packages=c("data.table","ecp","ggplot2"),.combine="list",.multicombine = TRUE) %do% {
    label = paste(x,collapse=" ")
    selection = with(files[[i]],node_id==x$node_id & termination_point==x$termination_point)
    if(label %in% names(tests)) {
      result = lapply(tests[[label]]$vars,function(y) {
        ds = files[[i]][selection,c(y,"fail","timestamp"),with=FALSE]
        ds[,cp:= 0]
        ds[tests[[label]]$estimates[c(-1,-length(tests[[label]]$estimates))],cp := 1]
        failure = as.numeric(ds[fail==1][["timestamp"]])
        cp = as.numeric(ds[cp==1][["timestamp"]])
        setnames(ds,c("timestamp",y),c("Time","val"))
        p = ggplot(ds) + aes(x=Time,y=val) + geom_line() + ylab(y) + ggtitle(paste(i,gsub(" ","\n",label),sep="\n")) +
          geom_vline(xintercept = failure,color="red",size=1.5) +
          geom_vline(xintercept = cp,color="blue",size=1)
        return(p)
      })
      names(result) = tests[[label]]$vars
      return(result)
    }
    else
      return(NULL)
  }
  rm(label,selection,result,x)

  names(plots) = names(tests)
  pdf(paste0(i,".pdf"))
  print(plots)
  dev.off()
  
  pbar$tick()
}
stopCluster(cl)

