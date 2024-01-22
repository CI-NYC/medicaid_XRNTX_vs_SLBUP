############################################
############################################
# Run the other 01* scripts - must be done before other cohort creation slides
# Author: Rachael Ross
#
# Output: NA (see individual called scripts)
############################################
############################################

source("/home/rr3551/moudr01/scripts/01_medicaidCE/00_paths_fxs.R")

tic()       
scriptpath <- "/home/rr3551/moudr01/scripts/01_medicaidCE/01_create_cohort/"

source(paste0(scriptpath,"01a_ouddx.R"))
print("01a_ouddx.R finished")

source(paste0(scriptpath,"01b_moud_fills.R"))
print("01b_moud_fills.R finished")

source(paste0(scriptpath,"01c_ambmoud.R"))
print("01c_ambmoud.R finished")

source(paste0(scriptpath,"01d_trtepisodes.R"))
print("01d_trtepisodes.R finished")
toc()
