############################################
############################################
# Run pre-processing scripts
# Author: Rachael Ross
############################################
############################################

#################
# Libraries
#################
library(arrow)
library(tidyverse)
library(lubridate)
library(tictoc)
library(sqldf)
library(data.table)

#################
# Paths
#################

datapath <- "/home/data/moud/global/"

# See if using full or sample from command line
dattype <- commandArgs(trailingOnly=TRUE)

# Set paths
if (dattype=="full") {
  #AWS/full
  inpath <- paste0("/mnt/processed-data/moud","/parsed/12692/")
  outpath <- paste0(datapath,"processed/")
} else if (dattype=="sample") {
  #AWS/sample
  inpath <- paste0(datapath,"sample/raw/")
  outpath <- paste0(datapath,"sample/processed/")
}

codes <- "/home/rr3551/moudr01/codes/global/" #For ED codes

#################
# Functions
#################

# Create ID that will link all tables
makelink <- function(dat){
  dat |> 
    mutate(link_id = ifelse(grepl("^\\s*$", BENE_ID)==TRUE|is.na(BENE_ID),
                            paste0(MSIS_ID,STATE_CD),
                            paste0(BENE_ID,STATE_CD)))
}

# To save partitioned parquets
save_multiparquet <- function(df,name,max_row){
  # If only 1 parquet needed
  if(nrow(df) <= max_row){
    write_parquet(df, 
                  paste0(outpath,name,"_0.parquet"))
  }
  
  # Multiple parquet files are needed
  if(nrow(df) > max_row){
    count = ceiling(nrow(df)/max_row)
    start = seq(1, count*max_row, by=max_row)
    end   = c(seq(max_row, nrow(df), max_row), nrow(df))
    
    for(j in 1:count){
      write_parquet(
        dplyr::slice(df, start[j]:end[j]), 
        paste0(outpath,name,"_",j-1,".parquet"))
    }
  }
}


