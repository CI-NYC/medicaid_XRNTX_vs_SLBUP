############################################
############################################
# Run the base covar scripts
# Author: Rachael Ross
############################################
############################################

library(tictoc)
tic()
scriptpath <- "/home/rr3551/moudr01/scripts/01_medicaidCE/02_baseline_covar/"
source(paste0(scriptpath,"01_base_psych.R"))
source(paste0(scriptpath,"02_base_sud.R"))
source(paste0(scriptpath,"03_base_comorbid.R"))
source(paste0(scriptpath,"04_base_oud.R"))
source(paste0(scriptpath,"05_base_meds.R"))
#source(paste0(scriptpath,"06_base_util.R")) 
source(paste0(scriptpath,"07_base_moud.R"))
toc()


