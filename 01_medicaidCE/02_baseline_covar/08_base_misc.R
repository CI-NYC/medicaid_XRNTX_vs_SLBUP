############################################
############################################
# Create baseline miscellaneous variables (epilepsy, homeless)
# Author: Rachael Ross
#
# Output: base_misc
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
# # Root paths
# datapath <- "/home/data/moud/global/"
# projpath <- "/home/data/moud/01_medicaidCE/"
# 
# # Set paths
# if (dattype=="full") {
#   #AWS/full
#   inraw <- paste0("/mnt/processed-data/moud","/parsed/12692/")
#   inproc <- paste0(datapath,"processed/")
#   outpath <- paste0(projpath,"intermediate/")
# } else if (dattype=="sample") {
#   #AWS/sample
#   inraw <- paste0(datapath,"sample/raw/")
#   inproc <- paste0(datapath,"sample/processed/")
#   outpath <- paste0(projpath,"sample/intermediate/")
# }
# 
# # Project code lists
# codes <- "/home/rr3551/moudr01/codes/01_medicaidCE/"

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
epilepsy_dx <- read_csv(paste0(codes,"epilepsy_dx.csv"))
housing_dx <- read_csv(paste0(codes,"housing_dx.csv"))

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


epi_dat <- filtercodes(epilepsy_dx) |> rename(misc_epilepsy = variable,
                                                misc_epilepsyalt = variable_alt)
house_dat <- filtercodes(housing_dx) |> rename(misc_homeless=variable,
                                               misc_homelessalt = variable_alt)

datlist <- list(epi_dat,house_dat)

base_misc <- reduce(datlist, ~left_join(.x,.y), by=c("link_id","mouddate"))

#table(base_misc$misc_epilepsyalt, useNA="ifany")

#################
# Save output
#################

saveRDS(base_misc,paste0(outpath,"base_misc.rds"))
print(paste0(nrow(base_misc),"in base_misc"))
toc()