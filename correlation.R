library(foreach)
library(doParallel)
library(data.table)
library(lubridate)
library(ggplot2)
library(progress)

setwd("C:/Users/AB58342/Documents/device failures/data")
file.names = grep("^h\\_",list.files(),value=TRUE)

files = lapply(file.names,function(x) list.files(paste(getwd(),x,sep="/"),full.names = TRUE))
names(files) = file.names
files = files[sapply(files,length) > 0]

readData = function(x) {
  # filter days to save time/space
  d=fread(x)
  d[,timestamp:=ymd_hms(timestamp)]
  # failtime = min(d[fail==1,timestamp])
  # if(!is.na(failtime))
  #   d = d[difftime(failtime,timestamp,units="hours") < 24*3]
  return(d)
}

pbar = progress_bar$new(total=length(files),format="[:bar] :current :percent :elapsed")
cormat = vector("list",length(files))
names(cormat) = names(files)

for(f in names(files)) {
  ds = Reduce(function(a,b) rbind(a,b,fill=TRUE),Map(readData,files[[f]]))[order(node_id,termination_point,timestamp)]
  
  #combine segments
  ds[,node_id := "ALL"]
  
  segments = unique(ds$node_id)
  cormat[[i]] = vector("list",length(segments))
  names(cormat[[i]]) = segments
  
  pdf(paste0("./failure_files/corrplots/",f,"_corr_matrix.pdf"),width=40,height = 25)
  for(j in segments) {
    vlist = setdiff(names(ds)[sapply(ds[node_id==j],class) %in% c("numeric","integer")],c("termination_point","node_ctrl","timestamp","fail","validity",
                                                                                                                  grep("card(sub)?type|mode$|type$|id$|secs$|qualifier$",names(ds),value=TRUE,ignore.case = TRUE,perl=TRUE)))
    v = sapply(ds[node_id==j,vlist,with=FALSE],var,na.rm=TRUE)
    vlist = vlist[v > 0 & !is.na(v)]
    
    matches = "PMDAVE|RxEdfaOprAve|RxEdfaLbcAve|ChanOchChromaticDispersionMin|ChanOchChromaticDispersionAve"
    matches = grep(matches,vlist,ignore.case = TRUE,perl=TRUE,value=TRUE)
    if(length(matches)>0)
      cat(f,": ",paste(matches,collapse="|"),"\n")
    
    if(length(vlist) > 0) {
      cormat[[i]][[j]] = cor(as.matrix(ds[node_id==j,c("fail",vlist),with=FALSE]),method="pearson",use="pairwise.complete.obs")
      melted.cormat = reshape2::melt(cormat[[i]][[j]])
      p = ggplot(melted.cormat) + theme(axis.text=element_text(size=14),
                                               title = element_text(size=20,face="bold"),
                                               legend.title = element_text(size=14)) +
        aes(x=Var1, y=Var2, fill=value) + geom_tile() + xlab("") + ylab("") + ggtitle(paste(i,j,sep="\n")) + scale_fill_gradient2(name="Correlation\n")
      #ggsave(p,filename = paste0(i,".png"),path = "corrplots/",device = "png",width = 40 ,height = 25 ,dpi = 300)
      print(p)
    }
  }
  dev.off()
  
  write.csv(as.data.frame(cormat[[i]][[j]]),paste0("./failure_files/corrplots/",f,"_corr_matrix.csv"))
  rm(ds)
  
  pbar$tick()
}




