data_dir="your datat dir here"

#Parallel call
main =function(dataframes,values,dir){
  iterations=length(dataframes)
  id=Sys.getpid()
  saveRDS(dataframes,paste0(dir,id,".RDS"))
  for (x in 1:iterations){
    computeResidualReturn(dataframes[[x]],values=x,dir)
  }
}
  

computeResidualReturn = function(dt, values,dir,trimOutliers = NULL,sparse =TRUE) {
  # Create date index mapping for simplicity
  df_dates = tibble(
    trans_date = seq(min(dt$trade_date),
                     max(dt$trade_date),
                     by='days')) %>% 
    # Drop weekends to save on dummies (discuss!)
    filter(!weekdays(trans_date) %in% c('Samstag', 'Sonntag')) %>%
    mutate(trans_period = row_number())
  

  dt = dt %>% 
    inner_join(df_dates, by = c('trade_date' = 'trans_date')) %>% 
    arrange(cusip, trade_date) %>% 
    group_by(cusip) %>% 
    mutate(t1 = trans_period,
           t0 = lag(trans_period, n = 1)) %>% 
    ungroup() %>% 
    # Make sure yield info is available
    filter(complete.cases(.) == TRUE)
  
  
  # trim outliers
  if (!is.null(trimOutliers)) {
    tmp1 = quantile(dt$y, trimOutliers, na.rm = TRUE)
    tmp2 = quantile(dt$y, 1-trimOutliers, na.rm = TRUE)
    dt = dt %>% 
      filter(y >= tmp1 & y <=tmp2)
  }
  
  time_start  = min(dt$t0)
  time_end    = max(dt$t1)
  time_diff   = time_end - time_start
  timeIdx = seq(time_start, time_end)
  
  # Fill in time matrix as sparse matrix
  idx = dt$t0 < timeIdx[1] & dt$t1 >= timeIdx[1]
  col=as.numeric(idx)
  sparse_new=as(col, "sparseMatrix")
  MatrixMaker2 = function(timeIdx,dt){
    for (tm in 2:length(timeIdx)) {
      # If column value is between start and end, set to 1 otherwise 0
      idx = dt$t0 < timeIdx[tm] & dt$t1 >= timeIdx[tm]
      col=as.numeric(idx)
      sparse_new=cbind(sparse_new, as(col, "sparseMatrix"))
    }
    return(sparse_new)
  }

  # OR Fill in time matrix as matrix
  library(tictoc)
  MatrixMaker = function(timeIdx,dt){
    time_matrix = array(0, dim = c(nrow(dt), time_diff + 1))
   # colnames(time_matrix) = paste0("time_", seq(time_start, time_end))
    for (tm in 1:length(timeIdx)) {
      # If column value is between start and end, set to 1 otherwise 0
      idx = dt$t0 < timeIdx[tm] & dt$t1 >= timeIdx[tm]
      time_matrix[idx, tm] = 1
      #  time_matrix[df_tmp$t0 == tm, tm - time_start] <- -1
      #  time_matrix[df_tmp$t1 == tm, tm - time_start] <- 1
    }
    return(time_matrix)
  }
 
  
  if (sparse == FALSE){
    print("matrix nonsparse")
    tic()
    time_matrix=MatrixMaker(timeIdx,dt)
    toc()
    return(time_matrix)
  }
  else{
    print("matrix sparse")
    tic()
    time_matrix=MatrixMaker2(timeIdx,dt)
    toc()
    id=Sys.getpid()
    saveRDS(time_matrix,paste0(dir,values,'__',id,".RDS"))
    #save y-val
    saveRDS(dt$y,paste0(dir,"y_vals/",values,'__',id,"_y.RDS"))
    return()
  }

 
 
}
#####################################################################################
#Running function parallelized
#non cluster version 
# print("Non cluster run")
# tic()
# q=main(sorted_dfs[[1]])
# toc()
# print("Non cluster run")
value=1
tic()
print("setting Up cluster")#executes in 15 min
cl=makeCluster(spec=5)
clusterEvalQ(cl,{library(dplyr)
  library(SparseM)
  library(Matrix)
  library(tictoc)})
clusterExport(cl,c("computeResidualReturn",'main','value','data_dir'))
print("Done setting Up cluster")
vals=clusterApply(cl,sorted_dfs,main,values=value,dir=data_dir)
stopCluster(cl)
print("done Executing")
toc()



###Data checks, confirming which rat/liq/mat cat went to which file

#Finding which chunk the files written to came from out of the larger list of list of dataframes
group_Finder = function(dataframes,larger_df_list){
  for (x in 1:length(dataframes)){
    if (identical(dataframes[[x]][[1]],larger_df_list[[1]])){
      return(x)
    }
  }
}

#for an individual file named via the session_id it was ran and its identifier of index, create a non sparse matrix and compare the results
compareMatricesIndiv= function(all_dfs,df_to_check,session_id){
  sparse_to_full=data.frame(as.matrix(readRDS(paste0(data_dir,df_to_check,"__",session_id,".RDS"))))
  group=readRDS(paste0(data_dir,session_id,".RDS"))
  group_number=group_Finder(all_dfs,group)
  trad_matrix=data.frame(computeResidualReturn(dt=all_dfs[[group_number]][[df_to_check]],value=1,sparse=FALSE))
  if (identical(sparse_to_full,trad_matrix)){
    print("pass")
  }
  else{
    print("fail")
  }
}

#run the comparison across all the dfs assigned to that session id
compareMatricesAll = function(all_dfs,session_id,dir){
  files=grep(as.character(session_id),list.files(dir),value=TRUE)
  files=gsub(paste0(as.character(session_id),".RDS"),"",files)
  number_files_on_core=max(as.numeric(gsub("__","",files)),na.rm=TRUE)
  for (x in 11:number_files_on_core){
    compareMatricesIndiv(all_dfs,x,session_id)
  }
  
}

#finding session id numbers
files=grep("__",list.files(data_dir),value=TRUE)
files=setdiff(list.files(data_dir),files)
files=gsub(".RDS","",files)
sessions=as.numeric(files)
sessions=sessions[!is.na(sessions)]
compareMatricesIndiv(sorted_dfs,10,sessions[[1]])
compareMatricesIndiv(sorted_dfs,10,sessions[[4]])
# 
#compareMatricesAll(sorted_dfs,sessions[[2]],data_dir)
#compareMatricesAll(sorted_dfs,sessions[[3]],data_dir)



#WHICH CATEGORY is which dataframe
saveNames_y= function(all_dfs,session_id){
  group=readRDS(paste0(data_dir,session_id,".RDS"))
  group_number=group_Finder(all_dfs,group)
  list_dfs=all_dfs[[group_number]]
  names=df_combo[[group_number]]
  data_dir_orig=data_dir
  data_dir=paste0(data_dir,"y_vals/")
  files=grep(as.character(session_id),list.files(data_dir),value=TRUE)
  files=gsub(paste0(as.character(session_id),"_y.RDS"),"",files)
  
  number_files_on_core=max(as.numeric(gsub("__","",files)),na.rm=TRUE)
  for (x in 1:number_files_on_core){
    file=readRDS(paste0(data_dir,x,'__',session_id,"_y.RDS"))
    saveRDS(file,paste0(data_dir,paste(names[[x]],collapse="___"),".RDS"))
    session_name = paste0(data_dir,x,'__',session_id,"_y.RDS")
    new_dir= paste0(data_dir_orig,"sessions/")
    system(paste0('mv ',session_name," ",new_dir))
  }
}
for (x in sessions){
  saveNames_y(sorted_dfs,x)
}



saveNames= function(all_dfs,session_id){
  group=readRDS(paste0(data_dir,session_id,".RDS"))
  group_number=group_Finder(all_dfs,group)
  list_dfs=all_dfs[[group_number]]
  names=df_combo[[group_number]]
  
  files=grep(as.character(session_id),list.files(data_dir),value=TRUE)
  files=gsub(paste0(as.character(session_id),".RDS"),"",files)
  number_files_on_core=max(as.numeric(gsub("__","",files)),na.rm=TRUE)
  for (x in 1:number_files_on_core){
    file=readRDS(paste0(data_dir,x,'__',session_id,".RDS"))
    saveRDS(file,paste0(data_dir,paste(names[[x]],collapse="___"),".RDS"))
    session_name = paste0(data_dir,x,'__',session_id,".RDS")
    new_dir= paste0(data_dir,"sessions/")
    system(paste0('mv ',session_name," ",new_dir))
  }
}
for (x in sessions){
  saveNames(sorted_dfs,x)
}

for (x in sessions){
  session_name=paste0(data_dir,x,".RDS")
  new_dir= paste0(data_dir,"sessions/")
  system(paste0('mv ',session_name," ",new_dir))
}
