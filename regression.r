#Performing the final regression
library(SparseM)
library(stringr)
library(doParallel)
data_dir="your data dir here"
files=grep(".RDS",list.files(data_dir),value = TRUE)
y_vals=grep(".RDS",list.files(paste0(data_dir,"y_vals/")),value = TRUE)
##########################################################################################
#grabbing a sample dataset from our 50 million row dataset
b=read_fst("sample.fst")

vals=str_split(files[[1]],"___")
vals[[1]][[3]]=gsub(".RDS","",vals[[1]][[3]])
right_df=b%>%
  filter(ratCat == vals[[1]][[1]], liqCat == vals[[1]][[2]], matCat == vals[[1]][[3]]) %>% 
  transmute(cusip, trade_date, y = DeltaYield_DurAdj1,ratCat,liqCat,matCat)


computeResidualReturn = function(dt, trimOutliers = NULL,sparse =TRUE,y_val=FALSE) {
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
  if(y_val == TRUE){
    return(dt)
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
    return(time_matrix)
    
  }
  
}
mat_sparse=computeResidualReturn(right_df,sparse =TRUE)
time_matrix=computeResidualReturn(right_df,sparse =FALSE)
y=computeResidualReturn(right_df,sparse =FALSE,y_val=TRUE)
y=as.matrix(y$y)
##########################################################################################
#This compares the output of a a sparse regression and a non-spare regressions
compare = function(sparse,nonsparse,keep_columns,width_original=NULL){
  X1=sparse[,keep_columns]
  #sparse answers
  coeffs1=MatrixModels:::lm.fit.sparse(X1, y)
  print("Sparse")
  print(coeffs1)
  #not sparse
  if(!is.null(width_original)){
    X2=nonsparse[,c(1:width_original)]
  }
  else{
    X2=nonsparse[,keep_columns]
  }
  
  lm(y~X2-1) -> lm.o
  sum.lmo <- summary(lm.o)
  #sum.lmo$coef <- sum.lmo$coef#[1:5,]
  print("LM:")
  print(sum.lmo$coef)
  print("diff")
  print(round(coeffs1-sum.lmo$coef),10)
}

#This uses QR decomposition to detect perfectly collinear columns
collinear_finder_by_column = function(matrix1,iter){
  tic()
  qr.X <- qr(matrix1[,1:iter], tol=1e-9, LAPACK = FALSE)
  toc()
  print((rnkX <- qr.X$rank))  ## 4 (number of non-collinear columns)
  (keep <- qr.X$pivot[seq_len(rnkX)])
  return(keep)
}

keep=collinear_finder_by_column(mat_sparse,10)
compare(mat_sparse,time_matrix,keep,width_original = 10)

#This searches all collumns for collinearity
collinear_finder = function(iter,indeces,matrix1,final_check=NULL){
  tic()
  if(is.null(final_check) ==TRUE){
    qr.X <- qr(matrix1[,indeces[iter]:indeces[iter+1]], tol=1e-9, LAPACK = FALSE)
    print((rnkX <- qr.X$rank))  ## 4 (number of non-collinear columns)
    (keep <- qr.X$pivot[seq_len(rnkX)])
    keep=seq(indeces[iter],indeces[iter+1])[keep]
  }
  else{
    qr.X <- qr(matrix1[,c(final_check)], tol=1e-9, LAPACK = FALSE)
    print((rnkX <- qr.X$rank))  ## 4 (number of non-collinear columns)
    (keep <- qr.X$pivot[seq_len(rnkX)])
    keep=final_check[keep]
  }
  toc()

  return(keep)
}


#This performs the collinear search using parralelization
regular_par = function(indeces,matrix1,iter){
  good_columns=c()
  for(x in 1:(length(indeces)-1)){
    print(paste(as.character(indeces[x]),as.character(indeces[x+1])))
    keep=collinear_finder(x,indeces,matrix1)
    good_columns=c(good_columns,keep)
  }
  new=c(indeces,indeces[x+1]+iter)
  if(indeces[x+1]+iter <= ncol(matrix1)){ #block for where it gets to last item in list to not go out of range
    print(paste(as.character(new[x+1]),as.character(new[x+2])))
    keep=collinear_finder(x+1,new,matrix1)
    good_columns=c(good_columns,keep)
  }
  good_columns=unique(good_columns)
  return(good_columns)
}

#This is the actual parallel call
filter_par= function(matrix1,iter=100,ncores){
  # Register a cluster of size cores
  registerDoParallel(cores=ncores)
  
  indeces=unique(c(seq(1,ncol(matrix1),by=iter),ncol(matrix1)))
  
  chunks=split(indeces,cut(seq(length(indeces)),ncores ))
  
  # foreach loop
  x=foreach(chunks=chunks,.combine=c, 
            .export = c("matrix1",'regular_par','collinear_finder'), #export functions
            .packages = c("dplyr","SparseM"))%dopar%
    regular_par(chunks,matrix1,iter=iter)
   #final test for full dataset collinearity
   good_columns=collinear_finder(1,x,matrix1,final_check=x)
  return(good_columns)
}

#First try it on the first 500 columns as a sanity check
tic()
vals=filter_par(mat_sparse[,c(1:500)],iter=50,ncores=5)
toc()
compare(mat_sparse,time_matrix,vals,width_original=600)

#Run final code for all columns of a dataset
Regression = function(X1,y_val,iter=100){
  #get noncollinear columns
  tic()
  vals=filter_par(X1,iter=iter,ncores=5)
  toc()
  print("Vals")
  print(vals)
  X1=X1[,vals]
  
  #run the regression
  coeffs1=MatrixModels:::lm.fit.sparse(X1, y_val)
  print("Sparse")
  print(coeffs1)
}

#Final execution
for (x in files){
  one_file=readRDS(paste0(data_dir,x))
  one_y=readRDS(paste0(data_dir,"y_vals/",x))
  if(dim(one_file)[[2]]<1000){
    iter=50
  }
  else{
    iter=100
  }
  Regression(one_file,one_y,iter=iter)
}




