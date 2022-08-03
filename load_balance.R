#This load balances by putting the largest dataframes first for each chunk, this speeds up parallelization.
LB_chunker = function(chunks=5,combo_names){
  nrows_df=as.numeric(lapply(dfs,nrow))
  dataframes=dfs[order(nrows_df,decreasing = TRUE)]
  combo_ordered=combo_names[order(nrows_df, decreasing=TRUE)]
  sorted_dfs=list()
  combo_ordered_list=list()
  for (x in 1:chunks){
    sorted_dfs=list.append(sorted_dfs,dataframes[seq(x,length(dataframes),chunks)])
    combo_ordered_list=list.append(combo_ordered_list,combo_ordered[seq(x,length(dataframes),chunks)])
  }
  return(list(sorted_dfs,combo_ordered_list))
}


results=LB_chunker(5,combo_names)
sorted_dfs=results[[1]]
df_combo=results[[2]]
rm(results)
rm(dfs)