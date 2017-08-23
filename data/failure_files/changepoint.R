library(foreach)
library(doParallel)
library(ecp)
library(bcp)
library(data.table)
library(lubridate)
library(ggplot2)
library(progress)

readData = function(x) {
  # filter days to save time/space
  d=fread(x)
  d[,timestamp:=ymd_hms(timestamp)]
  failtime = min(d[fail==1,timestamp])
  if(!is.na(failtime))
    d = d[difftime(failtime,timestamp,units="hours") < 24]
  return(d)
}

setwd("C:/Users/AB58342/Documents/device failures/data/failure_files")
file.names = grep("^h\\_.+csv$",list.files(),value=TRUE)
term.point = regexpr("^h\\_[A-Z]+",file.names,perl=TRUE)
term.point = regmatches(file.names,term.point)

file.names = split(file.names,term.point)

files = lapply(file.names,function(x) Reduce(function(a,b) rbind(a,b,fill=TRUE),Map(readData,x))[order(node_id,termination_point,timestamp)])

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

pval.next = 0.05
for(i in names(files)) {
  segments = unique(files[[i]][,.(node_id,termination_point)])
  
  tests = foreach(x = iter(segments,by="row"),.packages=c("data.table","ecp"),.multicombine=TRUE) %dopar% {
    print(x)
    selection = with(files[[i]],node_id==x$node_id & termination_point==x$termination_point)
    vlist = setdiff(names(files[[i]])[sapply(files[[i]][selection],class) %in% c("numeric","integer")],c("timestamp","fail","validity","NumSampleSecs"))
    v = sapply(files[[i]][selection,vlist,with=FALSE],var)
    vlist = vlist[v > 0 & !is.na(v)]
    result = e.divisive(as.matrix(files[[i]][selection,vlist,with=FALSE]),sig.lvl=pval.next,R=200,alpha=1)
    # result = e.agglo(X = as.matrix(files[[i]][selection,vlist,with=FALSE]),alpha = 1)
    result$vars = vlist
    result$name = paste(x,collapse = " ")
    return(result)
  }
  # sep = which(names(tests) ==  "name")
  # sep = cbind(c(1,sep[-length(sep)]+1),sep)
  # tests = apply(sep,1,function(x) tests[x[1]:x[2]])
  names(tests) = unlist(Map(function(x) x$name,tests))

  
  plots = foreach(x = iter(segments,by="row"),.packages=c("data.table","ggplot2"),.multicombine = TRUE) %do% {
    label = paste(x,collapse=" ")
    selection = with(files[[i]],node_id==x$node_id & termination_point==x$termination_point)
    if(label %in% names(tests)) {
      result = vector("list",length(tests[[label]]$vars))
      names(result) = tests[[label]]$vars
      for(y in tests[[label]]$vars) {
        ds = files[[i]][selection,c(y,"chassis_id","node_id","termination_point","fail","timestamp"),with=FALSE]
        ds[,cp:= 0]
        ds[tests[[label]]$estimates[c(-1,-length(tests[[label]]$estimates))],cp := 1]
        failure = as.numeric(ds[fail==1][["timestamp"]])
        cp = as.numeric(ds[cp==1][["timestamp"]])

        p = ggplot(ds) + aes_string(x="timestamp",y=y) + geom_line() + ylab(y) + xlab("Time") + ggtitle(paste(i,gsub(" ","\n",label),sep="\n")) +
          geom_vline(xintercept = failure,color="red",size=1.5) + geom_vline(xintercept = cp,color="blue",size=1)

        result[[y]] = list(plot=p,data=ds)
      }
      return(result)
    }
    else
      return(NULL)
  }
  names(plots) = names(tests)
  rm(label,selection,result,x,y,ds,p,failure,cp)

  
  pdf(paste0(i,"_changepoint_plots.pdf"))
  Map(function(x) Map(function(y) print(y$plot),x),plots)
  dev.off()
  
  # data output
  out.data = Reduce(function(a,b) rbind(a,b,fill=TRUE),Map(function(x) Reduce(function(a,b) merge(a,b[,],by=c("timestamp","chassis_id","node_id","termination_point","fail","cp")),Map(function(y) y$data,x)),plots))
  fwrite(out.data,paste0(i,"_changepoint_data.csv"))
  
  pbar$tick()
}
stopCluster(cl)

