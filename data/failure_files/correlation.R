library(foreach)
library(doParallel)
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
  # failtime = min(files[[i]][fail==1,timestamp])
  # files[[i]] = files[[i]][difftime(failtime,timestamp,units="hours") < 48]
  # rm(failtime)
}

varlist = fread("corr vars.csv",stringsAsFactors = FALSE)

cormat = vector("list",length(files))
names(cormat) = names(files)

pbar = progress_bar$new(total=length(files),format="[:bar] :current :percent :elapsed")
for(i in names(cormat)) {
  cormat[[i]] = cor(as.matrix(files[[i]][,c("fail",varlist[h==i,variable]),with=FALSE]),method="pearson",use="pairwise.complete.obs")
  pbar$tick()
}

melted.cormat = lapply(cormat,reshape2::melt)

for(i in names(melted.cormat)) {
  p = ggplot(data = melted.cormat[[i]]) + theme(axis.text=element_text(size=14),
                                                title = element_text(size=20,face="bold"),
                                                legend.title = element_text(size=14)) +
    aes(x=Var1, y=Var2, fill=value) + geom_tile() + xlab("") + ylab("") + ggtitle(i) + scale_fill_gradient2(name="Correlation\n")
  ggsave(p,filename = paste0(i,".png"),device = "png",width = 40 ,height = 25 ,dpi = 400)
}


# try by terminatin point block
cormat = vector("list",length(files))
names(cormat) = names(files)
pbar = progress_bar$new(total=length(files),format="[:bar] :current :percent :elapsed")
for(i in names(cormat)) {
  segments = unique(files[[i]]$termination_point)
  cormat[[i]] = vector("list",length(segments))
  names(cormat[[i]]) = segments
  
  pdf(paste0(i,"_corr_matrix",".pdf"),width=40,height = 25)
  for(j in segments) {
    vlist = setdiff(names(files[[i]])[sapply(files[[i]][termination_point==j],class) %in% c("numeric","integer")],c("termination_point","node_ctrl","timestamp","fail","validity",
                                                                                              grep("card(sub)?type|mode$|type$|id$|secs$|qualifier$",names(files[[i]]),value=TRUE,ignore.case = TRUE,perl=TRUE)))
    v = sapply(files[[i]][termination_point==j,vlist,with=FALSE],var)
    vlist = vlist[v > 0 & !is.na(v)]
    if(length(vlist) > 0) {
      cormat[[i]][[j]] = cor(as.matrix(files[[i]][termination_point==j,c("fail",vlist),with=FALSE]),method="pearson",use="pairwise.complete.obs")
      melted.cormat = reshape2::melt(cormat[[i]][[j]])
      p = ggplot(data = melted.cormat) + theme(axis.text=element_text(size=14),
                                                    title = element_text(size=20,face="bold"),
                                                    legend.title = element_text(size=14)) +
        aes(x=Var1, y=Var2, fill=value) + geom_tile() + xlab("") + ylab("") + ggtitle(paste(i,j,sep="\n")) + scale_fill_gradient2(name="Correlation\n")
      #ggsave(p,filename = paste0(i,".png"),path = "corrplots/",device = "png",width = 40 ,height = 25 ,dpi = 300)
      print(p)
    }
  }
  dev.off()
  pbar$tick()
}


