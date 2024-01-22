############################################
############################################
# Create baseline comorbid psychiatric conditions variables
# Author: Rachael Ross
#
# Output: base_psych 
############################################
############################################

source("/home/rr3551/moudr01/scripts/01_medicaidCE/00_paths_fxs.R")

# #################
# # Libraries
# #################
# library(arrow)
# library(tidyverse)
# library(lubridate)
# library(tictoc)
# library(sqldf)
# library(data.table)
# 
# #################
# # Paths
# #################
# 
# codes <- "/home/rr3551/moudr01/codes/"
# 
# #AWS/full
# # inraw <- "/mnt/data/disabilityandpain-r/moud/parsed/12692/"
# # inproc <- "/home/rr3551/moudr01/data/processed/"
# # outpath <- "/home/rr3551/moudr01/data/01_medicaidCE/intermediate/"
# 
# # #AWS/sample
# inraw <- "/home/rr3551/moudr01/data/sample/"
# inproc <- "/home/rr3551/moudr01/data/sample/processed/"
# outpath <- "/home/rr3551/moudr01/data/sample/01_medicaidCE/intermediate/"

#################
# Load files
#################
tic()

# Load fill data
list <- list.files(outpath, pattern = "*imoud_dates*", recursive = TRUE) 
imoud_dates <- open_dataset(paste0(outpath, list), format="parquet") |> 
  select(link_id,mouddate,enroll_start,enrollalt_start) |>
  collect() |>
  distinct()

# Diagnosis codes
list <- list.files(inproc, pattern = "*dx_.*\\.parquet$", recursive = TRUE) 
dxs <- open_dataset(paste0(inproc, list), format="parquet") |>
  filter(link_id %in% imoud_dates$link_id) |> collect()

# Code lists
depression_dx <- read_csv(paste0(codes,"depression_dx.csv"))
anxiety_dx <- read_csv(paste0(codes,"anxiety_dx.csv"))
bipolar_dx <- read_csv(paste0(codes,"bipolar_dx.csv"))
adhd_dx <- read_csv(paste0(codes,"adhd_dx.csv"))
ptsd_dx <- read_csv(paste0(codes,"ptsd_dx.csv"))
psychoth_dx <- read_csv(paste0(codes,"psychoth_dx.csv"))
psych_dx <- read_csv(paste0(codes,"psych_dx.csv"))

#################
# Processing
#################

setDT(imoud_dates) 

# This coding is flexible enough to include matching on parent codes
filtercodes <- function(codes){
  codelist <- str_c(codes$code, collapse="|")
  
  dat <- dxs |> 
    filter(str_detect(DGNS_CD,codelist))
  
  setDT(dat)
  setkey(dat,link_id,SRVC_BGN_DT,SRVC_END_DT) 
  
  withdx <-  foverlaps(imoud_dates, dat, 
                       by.x = c("link_id", "enroll_start", "mouddate"),
                       by.y = c("link_id", "SRVC_BGN_DT", "SRVC_END_DT"),
                       type = "any", mult = "all", nomatch=NULL)[,`:=` (variable = 1,
                                                                        variable_alt = case_when(SRVC_END_DT>enrollalt_start ~ 1,
                                                                                                 .default = 0))
                       ][,.(variable=max(variable),
                            variable_alt=max(variable_alt)), by=.(link_id,mouddate)]
  
imoud_dates |>
    select(link_id,mouddate) |>
    left_join(withdx, by=c("link_id","mouddate")) |>
    mutate(variable = ifelse(is.na(variable),0,variable),
           variable_alt = ifelse(is.na(variable_alt),0,variable_alt)) 
}


dep_dat <- filtercodes(depression_dx) |> rename(psy_depression = variable,
                                                psy_depressionalt = variable_alt)
anx_dat <- filtercodes(anxiety_dx) |> rename(psy_anxiety=variable,
                                             psy_anxietyalt = variable_alt)
bip_dat <- filtercodes(bipolar_dx) |> rename(psy_bipolar=variable,
                                             psy_bipolaralt = variable_alt)
adhd_dat <- filtercodes(adhd_dx) |> rename(psy_adhd=variable,
                                           psy_adhdalt = variable_alt)
ptsd_dat <- filtercodes(ptsd_dx) |> rename(psy_ptsd=variable,
                                           psy_ptsdalt = variable_alt)
oth_dat <- filtercodes(psychoth_dx) |> rename(psy_psychoth=variable,
                                              psy_psychothalt = variable_alt)
psy_dat <- filtercodes(psych_dx) |> rename(psy_psych=variable,
                                           psy_psychalt = variable_alt)

datlist <- list(dep_dat,anx_dat,bip_dat,adhd_dat,ptsd_dat,oth_dat,psy_dat)

base_psych <- reduce(datlist, ~left_join(.x,.y), by=c("link_id","mouddate"))

#################
# Save output
#################

saveRDS(base_psych,paste0(outpath,"base_psych.rds"))
print(paste0(nrow(base_psych),"in base_psych"))
toc()