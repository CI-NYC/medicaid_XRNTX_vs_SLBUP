############################################
############################################
# Paths/functions for cohort creation scripts
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

# See if using full or sample from command line
#print(commandArgs(trailingOnly=TRUE))
dattype <- commandArgs(trailingOnly=TRUE)
#print(dattype)

# Root paths
datapath <- "/home/data/moud/global/"
projpath <- "/home/data/moud/01_medicaidCE/"

# Set paths
if (dattype=="full") {
  #AWS/full
  inraw <- paste0("/mnt/processed-data/moud","/parsed/12692/")
  inproc <- paste0(datapath,"processed/")
  outpath <- paste0(projpath,"intermediate/")
} else if (dattype=="sample") {
  #AWS/sample
  inraw <- paste0(datapath,"sample/raw/")
  inproc <- paste0(datapath,"sample/processed/")
  outpath <- paste0(projpath,"sample/intermediate/")
}

#print(inraw)

# Project code lists
codes <- "/home/rr3551/moudr01/codes/01_medicaidCE/"

#################
# Assumptions
#################


enrollmo <- 6 #Months required for continuous enrollment
enrollmo_alt <- 3 #Months required for continuous enrollment, alternate
washoutday <- 180 #Days for trt washout
washoutday_alt <- 90 #Days for trt washout, alternate
mindayscover <- 15 #minimum days with coverage in a month to be considered enrolled
ntx_priormbuse <- 14 #days prior to ntx where mth/bup allowed (for detox)
bup_priormuse <- 0 #days prior to bup where mth allowed (for detox)
ambcode_days <- 180 #max number of days to look for moud to determine med for ambiguous codes
shortcourse <- 7 #days used to define short course trt to be excluded
earlywindow <- 14 #days in window used to asses short course and early outcomes
ntxinjlength <- 30 #days of treatment for bup or ntx injection
bupndciposlength <- 1 #days of treatment for bup ndcs from os or ip files
bupimplength <- 180 #days of treatment for bup implant
moudgrace <- 31 #days used to determine when treatment discontinued
moudgrace_alt <- 14 #days used to determine when treatment discontinued



