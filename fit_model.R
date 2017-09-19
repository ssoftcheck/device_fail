source("setup.R")

# read data
files = grep(paste0("/",termination_point,"\\_(?!changepoint).+csv$"),list.files(file_location,full.names=TRUE),value=TRUE,perl = TRUE)
ds = Reduce(function(a,b) rbind(a,b,fill=TRUE),Map(readData,files))
ds[,fail:=factor(fail)]
nodes = ds[,unique(node_id)]

# create lag variables and add to predictor set
interval = 4 # 1 hour
lags = 24 # 1 day
l = seq(interval,lags,interval)
for(v in vars) {
  newvars = paste0(v,"_lag",l)
  ds[,(newvars) := shift(get(v),l),by=.(node_id,termination_point)]
  var.formula = update(var.formula,paste(c("~.",newvars),collapse="+"))
  vars = union(vars,newvars)
}

# make folds
set.seed(5)
ds[,fold := sample(1:n_folds,nrow(ds),replace=TRUE)]

# evaluate parameters with cross validation
params = expand.grid(objective="binary:logistic",subsample=c(0.5,0.7,0.9),max.depth=c(2,4,8),eta=c(0.001,0.01),min.child.weight=c(1,2,3),colsample.bytree=c(0.5,0.7,0.9),stringsAsFactors = FALSE,KEEP.OUT.ATTRS = FALSE)
params = as.data.table(params)

# establish parallel workers processes and evaluate parameter grid
cl = makeCluster(detectCores()/2)
registerDoSNOW(cl)
opts = list(progress=function(incr) setTkProgressBar(pb,value = incr,label=sprintf("%3.2f%%",100*incr/nrow(params))))

perf.final = foreach(cvf = min(ds$fold):max(ds$fold),.combine="rbind",.multicombine=TRUE) %do% {
  trainset = which(ds[,fold] != cvf)
  testset = which(ds[,fold] == cvf)
  
  train = SMOTE(var.formula,data=ds[trainset,c("fail",vars),with=FALSE],perc.over = 5000,perc.under = 100)
  
  cat("Fold ",cvf,"\n")
  cat("old event rate: ",ds[trainset,mean(as.integer(fail)-1)],"\n")
  cat("new event rate: ",train[,mean(as.integer(fail)-1)],"\n")
  
  train = xgb.DMatrix(as.matrix(train[,vars,with=FALSE]),label = as.integer(train[,fail])-1)
  test = xgb.DMatrix(as.matrix(ds[testset,vars,with=FALSE]),label = as.integer(ds[testset,fail])-1)
  xgb.DMatrix.save(train,"train.matrix")
  xgb.DMatrix.save(test,"test.matrix")
  rm(train,test)
  
  pb = tkProgressBar(min = 0,max=max(ds$fold),title =paste("Progress Fold",cvf),label = "0%")
  result = foreach(pnow=iter(params,by="row"),.combine="rbind",.packages=c("xgboost","data.table","ROCR"),.multicombine=TRUE,.options.snow=opts) %dopar% {
    fit = xgb.train(data=xgb.DMatrix("train.matrix"),nrounds=1000,save_period=NULL,params=as.list(pnow),verbose=0)
    test = xgb.DMatrix("test.matrix")
    perf = lapply(seq(100,1000,100),function(ntree) {
      pred = predict(fit,test,ntreelimit = ntree)
      assess = lapply(seq(0.1,0.9,0.1),function(cut) lapply(list(data.table(a=ds[testset,fail],p=pred)),function(x) {
        data.table(fold=cvf,ntree=ntree,cutoff=cut,auc=auc(x$a,x$p),phi=phi(x$a,x$p,cut),acc=accuracy(x$a,x$p,cut),sens=sensitivity(x$a,x$p,cut),spec=specificity(x$a,x$p,cut))
      })[[1]])
      return(rbindlist(assess))
    })
    return(rbindlist(perf))
  }
  file.remove(c("train.matrix","test.matrix"))
  close(pb)
  return(result)
}
stopCluster(cl)


# summarize results across validation folds
perf.summary = cbind(params[rep(1:nrow(params),times=max(ds$fold),each=9*n_folds)],perf.final)
perf.summary[,balacc:= (sens+spec)/2]
perf.summary = perf.summary[,lapply(.SD,mean),by=c(names(params),"ntree","cutoff"),.SDcols=c("auc","phi","acc","sens","spec","balacc")]
save(perf.final,perf.summary,file=paste0(termination_point,".perf.summary.rdata"))
fwrite(perf.final,paste0(termination_point,"_cross_validation_performance"))

best.params = perf.summary[get(metric)==max(get(metric)),.(objective,subsample,max.depth,eta,min.child.weight,colsample.bytree)][1]
best.ntree = perf.summary[get(metric)==max(get(metric))][1,ntree]
best.cutoff = perf.summary[get(metric)==max(get(metric))][1,cutoff]

# train final model. save training data and final model fit
set.seed(5)
train.full = SMOTE(var.formula,data=ds[,c("fail",vars),with=FALSE],perc.over = 5000,perc.under = 100)
save(train.full,file=paste0(termination_point,"_full_train.rdata"))
train.full = xgb.DMatrix(as.matrix(train[,vars,with=FALSE]),label = as.integer(train[,fail])-1)
full.fit = xgb.train(data=train.full,nrounds=best.ntree,save_period=NULL,params=as.list(best.params))
xgb.save(full.fit,paste0(termination_point,"_full_fit.xgb"))
ds[,pred := predict(full.fit,xgb.DMatrix(as.matrix(ds[,vars,with=FALSE])))]
ds[,pred_class := ifelse(pred > best.cutoff,1,0)]


# top N variable importance 
full.imp = xgb.importance(feature_names = vars,model=full.fit)
pdf(paste0(termination_point,"_variable_importance.pdf"))
xgb.ggplot.importance(importance_matrix = full.imp,top_n = 15)
dev.off()


# create partial dependence
load(file=paste0(termination_point,"_full_train.rdata"))
vp = train.full[,lapply(.SD,function(x) quantile(x,c(0.02,seq(0.1,0.9,0.1),0.98),na.rm=TRUE)),.SDcols=top]

opts = list(progress=function(incr) setTkProgressBar(pb,value = incr,label=sprintf("%3.2f%%",100*incr/length(top))))
pb = tkProgressBar(min = 0,max=length(top),title = "Progress",label = "0%")

cl = makeCluster(detectCores()/2)
registerDoSNOW(cl)
pd = foreach(x = names(vp),.packages=c("data.table","xgboost","pdp"),.options.snow=opts) %dopar% {
  partial(object = full.fit,pred.var=x,pred.grid = vp[,x,with=FALSE],chull=TRUE,prob=TRUE,train=train[,setdiff(names(train),"fail"),with=FALSE],plot=FALSE)
}
names(pd) = top
stopCluster(cl)
close(pb)

# plot partial dependence
pdf(paste(termination_point,"top 15 parital dependence plots.pdf"))
for(i in names(pd))
  print(ggplot(pd[[i]]) + aes_string(x=i,y="yhat") + geom_point() + geom_line() + geom_smooth() + ylab("Failure Probability"))
dev.off()


# plot predicted probabilities over time leading to failure
hasfail = ds[fail==1,.(start=min(timestamp)),by=.(node_id,termination_point)]
pdf(paste(termination_point,"failure prediction over time.pdf"))
for(i in 1:nrow(hasfail)) {
  plt = ggplot(ds[termination_point==hasfail[i,termination_point] & node_id==hasfail[i,node_id] & 
                    difftime(hasfail[i,start],timestamp,units="hours") <= 24 & difftime(hasfail[i,start],timestamp,units="hours") >= -4]) + 
    aes(x=timestamp,y=pred) + geom_point() + geom_line() + geom_vline(xintercept=as.numeric(hasfail[i,start]),lty=2,color="red",size=2) + geom_smooth(method="loess") + 
    scale_y_continuous(limits = c(0,1)) +
    xlab("Time") + ylab("Failure Probability") + ggtitle(paste0("Node ",gsub("(.+)@(.+)","\\1",hasfail[i,node_id]),"\n","Termination Point ",hasfail[i,termination_point]))
  print(plt)
}
dev.off()