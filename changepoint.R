source("setup_changepoint.R")

files = grep(termination_point,list.files(file_location,full.names=TRUE),value=TRUE,perl = TRUE)
file.dates = gsub("(.+_)(\\d{4}-\\d{2}\\-\\d{2})(.csv)$","\\2",files)
files = split(files,file.dates)
ds = lapply(files,function(x) Reduce(function(a,b) rbind(a,b,fill=TRUE),Map(readData,x)))

pbar = progress_bar$new(total=length(node_id),format="[:bar] :current :percent :elapsed")
cl = makeCluster(detectCores()/2)
registerDoSNOW(cl)
opts = list(progress=function(incr) setTkProgressBar(pb,value = incr,label=sprintf("%3.2f%%",100*incr/nrow(segments))))
pval.next = 0.05
for(item in names(ds)) {
  each = ds[[item]]
  vars = unlist(lapply(sapply(each,class),paste,collapse=""))
  vars = names(vars[!(vars %in% c("character","factor","logical"))])
  vars = setdiff(vars,c("timestamp","fail","validity","NumSampleSecs","chassis_id","CardType","alarms"))

  segments = unique(each[,.(node_id,termination_point)])
  
  pb = tkProgressBar(min = 0,max=nrow(segments),title = paste("Changepoint Progress",item),label = "0%")
  tests = foreach(x = iter(segments,by="row"),.packages=c("data.table","ecp"),.multicombine=TRUE,.options.snow=opts) %dopar% {
    selection = with(each,node_id==x$node_id & termination_point==x$termination_point)
    
    varcheck = melt(each[selection,lapply(.SD,function(z) c(miss=sum(is.na(z)),nomiss=sum(!is.na(z)),variance=var(z,na.rm=TRUE))),by=node_id,.SDcols=vars])
    varcheck = varcheck[,.(miss=value[1],nomiss=value[2],variance=value[3]),by=.(node_id,variable)]
    varcheck = varcheck[nomiss > 0 & variance > 0,variable]
    
    result = e.divisive(as.matrix(each[selection,varcheck,with=FALSE]),sig.lvl=pval.next,R=200,alpha=1)
    result$vars = varcheck
    result$name = paste(x,collapse = " ")
    return(result)
  }
  close(pb)
  names(tests) = unlist(Map(function(x) x$name,tests))

  pb = tkProgressBar(min = 0,max=nrow(segments),title = paste("Plot Progress",item),label = "0%")
  plots = foreach(x = iter(segments,by="row"),.packages=c("data.table","ggplot2","data.table"),.multicombine = TRUE,.options.snow=opts) %dopar% {
    label = paste(x,collapse=" ")
    selection = with(each,node_id==x$node_id & termination_point==x$termination_point)
    if(label %in% names(tests)) {
      result = vector("list",length(tests[[label]]$vars))
      names(result) = as.character(tests[[label]]$vars)
      subd = each[selection,c(as.character(tests[[label]]$vars),"chassis_id","node_id","termination_point","fail","timestamp"),with=FALSE]
      subd[,cp:=0]
      for(y in as.character(tests[[label]]$vars)) {
        subd[tests[[label]]$estimates[c(-1,-length(tests[[label]]$estimates))],cp := 1]
        failure = as.numeric(subd[fail==1,timestamp][1])
        cp = as.numeric(subd[cp==1,timestamp])

        p = ggplot(subd) + aes_string(x="timestamp",y=y) + geom_line() + ylab(y) + xlab("Time") + ggtitle(paste(label,gsub(" ","\n",label),sep="\n")) +
          geom_vline(xintercept = failure,color="red",size=1.5) + geom_vline(xintercept = cp,color="blue",size=1)

        result[[y]] = list(plot=p)
      }
      result$data = subd
      return(result)
    }
    else
      return(NULL)
  }
  close(pb)
  names(plots) = names(tests)

  pdf(paste0(item,"_changepoint_plots.pdf"))
  Map(function(x) Map(function(y) {
    if(!is.null(y))
      print(y$plot)
    },x),plots)
  dev.off()

  # data output
  out.data = Reduce(function(a,b) rbind(a,b,fill=TRUE),Map(function(x) x$data,plots))
  fwrite(out.data,paste0(item,"_changepoint_data.csv"))
  
  pbar$tick()
}
stopCluster(cl)

