############################################
############################################
# Create baseline SUD variables
# Author: Rachael Ross
#
# Output: base_sud
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
alcohol_dx <- read_csv(paste0(codes,"alcohol_dx.csv"))
cannabis_dx <- read_csv(paste0(codes,"cannabis_dx.csv"))
sedative_dx <- read_csv(paste0(codes,"sedative_dx.csv"))
cocaine_dx <- read_csv(paste0(codes,"cocaine_dx.csv"))
amph_dx <- read_csv(paste0(codes,"amph_dx.csv"))
othsud_dx <- read_csv(paste0(codes,"othsud_dx.csv"))
stimoverdose_dx <- read_csv(paste0(codes,"stimoverdose_dx.csv"))

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


alc_dat <- filtercodes(alcohol_dx) |> rename(sud_alcohol=variable,
                                             sud_alcoholalt=variable_alt)
can_dat <- filtercodes(cannabis_dx) |> rename(sud_cannabis=variable,
                                              sud_cannabisalt=variable_alt)
sed_dat <- filtercodes(sedative_dx) |> rename(sud_sedative=variable,
                                              sud_sedativealt=variable_alt)
coc_dat <- filtercodes(cocaine_dx) |> rename(sud_cocaine=variable,
                                             sud_cocainealt=variable_alt)
amph_dat <- filtercodes(amph_dx) |> rename(sud_amphetamine=variable,
                                           sud_amphetaminealt=variable_alt)
oth_dat <- filtercodes(othsud_dx) |> rename(sud_other=variable,
                                            sud_otheralt=variable_alt)
stim_dat <- filtercodes(stimoverdose_dx) |> rename(sud_stimod=variable,
                                                   sud_stimodalt=variable_alt)

datlist <- list(alc_dat,can_dat,sed_dat,coc_dat,amph_dat,oth_dat,stim_dat)

base_sud <- reduce(datlist, ~left_join(.x,.y), by=c("link_id","mouddate"))

#################
# Save output
#################

saveRDS(base_sud,paste0(outpath,"base_sud.rds"))
print(paste0(nrow(base_sud),"in base_sud"))
toc()