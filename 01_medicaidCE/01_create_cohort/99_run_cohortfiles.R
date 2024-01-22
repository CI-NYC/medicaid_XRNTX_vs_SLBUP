############################################
############################################
# Run the cohort scripts for incl and exclusion
# Author: Rachael Ross
############################################
############################################

scriptpath <- "/home/rr3551/moudr01/scripts/01_medicaidCE/01_create_cohort/"
source(paste0(scriptpath,"01_incl_fills.R"))
source(paste0(scriptpath,"02_incl_age.R"))
source(paste0(scriptpath,"03_excl_contenroll.R"))
source(paste0(scriptpath,"04_excl_ouddx.R"))
source(paste0(scriptpath,"05_excl_preg.R"))
source(paste0(scriptpath,"06_excl_cancer.R"))
source(paste0(scriptpath,"07_excl_psych.R"))
source(paste0(scriptpath,"08_excl_pall.R"))
source(paste0(scriptpath,"09_excl_prioruse.R"))
source(paste0(scriptpath,"10_excl_tooshort.R"))
source(paste0(scriptpath,"11_excl_earlyout.R"))

