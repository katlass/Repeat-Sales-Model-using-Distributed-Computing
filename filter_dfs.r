#Loading Data

# tmpData=read_fst("tmpData.fst")
# #make a 50,000,000 row dummy to work with 
# b=purrr::map_dfr(seq_len(500), ~tmpData)
# #fst::write_fst(b, 'sample.fst') #read it later
b=read_fst("sample.fst")

#get unique categories
unique_ratCat=unique(b$ratCat)
unique_liqCat=unique(b$liqCat)
unique_matCat=unique(b$matCat)

#get all unique combos
combos=list()
for (x in unique_ratCat){
  for (y in unique_liqCat){
    for (z in unique_matCat){
      combos= list.append(combos,c(x,y,z))
    }
  }
}
print(paste(length(combos),"unique combos"))

#filter into smaller dfs that meet prior conditions
selectFilter = function(df,params){
  result=list(df%>%
    filter(ratCat == params[[1]], liqCat == params[[2]], matCat == params[[3]]) %>% 
    transmute(cusip, trade_date, y = DeltaYield_DurAdj1,ratCat,liqCat,matCat))
  return(result)
}

#Determine number cores
ncores=detectCores(logical=FALSE)-1 #I have 32
print(paste(ncores,"cores"))

#Filtering the dataframe in parallel
filter_par= function(ncores,combos,data){
  # Register a cluster of size cores
  registerDoParallel(cores=ncores)
  # foreach loop
  x=foreach(ratCat=combos,.combine=c, 
            .export = c("b","combos","selectFilter"), #export functions
            .packages = c("dplyr"))%dopar%
    #combos
    selectFilter(data,params=ratCat)
  return(x)
}

#Filtering the dataframe in sequentially
filter_seq= function(ncores,combos,data){
  # Register a cluster of size cores
  registerDoParallel(cores=ncores)
  # foreach loop
  x=foreach(ratCat=combos,.combine=c, 
            .export = c("b","combos","selectFilter"), #export functions
            .packages = c("dplyr"))%do%
    #combos
    selectFilter(data,params=ratCat)
  return(x)
}

# Benchmark
print("Evaluating sequential vs parallel")
microbenchmark(filter_seq(ncores,combos,data=b), 
                filter_par(ncores, combos,data=b), #takes about 70 seconds
                times = 1)
print("Done Evaluating sequential vs parallel")
 
#filter into dataframes meeting unique conditions
print("True filtering")
tic()
dfs=filter_par(ncores, combos,data=b)
toc()
print("True filtering done")

#rating/liquidity/maturity combos in order, dfs in order
combo_names=lapply(dfs,function(x) return(c(paste(x[1,4:6]))))
dfs=lapply(dfs,function(x) return(select(x,c('cusip', 'trade_date', 'y'))))


#Data Checks:
DataChecks = function(expected_val,true_val){
  if (!all.equal(expected_val,true_val)){
    print("Fail")
  }
  else{
    print("Pass")
  }
}
print("Data checks:")
DataChecks(selectFilter(b,combos[[1]])[[1]][,c(1:3)],dfs[[1]])
DataChecks(selectFilter(b,combos[[2]])[[1]][,c(1:3)],dfs[[2]])
DataChecks(selectFilter(b,combos[[3]])[[1]][,c(1:3)],dfs[[3]])
DataChecks(selectFilter(b,combos[[71]])[[1]][,c(1:3)],dfs[[71]])
DataChecks(selectFilter(b,combos[[72]])[[1]][,c(1:3)],dfs[[72]])
rm(b)
