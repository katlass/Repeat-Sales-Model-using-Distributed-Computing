#repeat sales model
rm(list=ls()) 
library(fst)
library(dplyr)
library(parallel)
library(microbenchmark)
library(rlist)
library(foreach)
library(doParallel)
library(tictoc)

wd="place your wd here"
setwd(wd)

source("filter_dfs.r")
source("load_balance.R")
source("matrix_maker.R")
source("regression.r")