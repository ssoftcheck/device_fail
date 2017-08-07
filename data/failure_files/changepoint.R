library(ecp)
library(strucchange)
library(data.table)
library(lubridate)
library(ggplot2)
library(progress)

setwd("C:/Users/AB58342/Documents/device failures/data/failure_files")
files = grep("csv$",list.files(),value=TRUE)
files = lapply(files,fread)
for(i in seq(length(files))) {
  files[[i]][,timestamp := ymd_hms(timestamp)]
}

plist = list()
pbar = progress_bar$new(total=length(files),format="[:bar] :current :percent :elapsed")
for(i in seq(length(files))) {
  has.fail = files[[i]][,.(failsum=sum(fail)),termination_point][failsum > 0]
  for(tp in has.fail$termination_point) {
    vlist = setdiff(names(files[[i]])[sapply(files[[i]],class) %in% c("numeric","integer")],c("timestamp","fail"))
    v = sapply(files[[i]][termination_point==tp,vlist,with=FALSE],var)
    vlist = vlist[which(v > 0 & !is.na(v))]
    if(length(vlist) > 0)
      changepoints = e.divisive(as.matrix(files[[i]][termination_point==tp,vlist,with=FALSE]))
    for(j in vlist) {
      cpoints = files[[i]][termination_point==tp,timestamp]
      cpoints = as.numeric(cpoints[with(changepoints,estimates[-1][p.values < 0.05])])
       plt = ggplot(files[[i]][termination_point==tp,.(s=get(j),fail,timestamp)]) + 
        aes(x=timestamp,y=s) + geom_line() + ylab(j) + ggtitle(tp) + 
        geom_vline(xintercept=cpoints,color="blue",lty=2) +
        geom_vline(xintercept=files[[i]][termination_point==tp & fail==1,as.numeric(timestamp)],color="red",lty=2)
       plist[[length(plist)+1]] = plt
    }
    pbar$tick()
  }
}
lapply(plist,function(x) ggsave(filename=paste0(x$labels$title," ",x$labels$y,".png"),x,device = "png"))
