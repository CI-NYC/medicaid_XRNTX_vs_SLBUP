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
  select(link_id,mouddate,enroll_start, enrollalt_start) |>
  collect() |>
  distinct()

# ndcs
list <- list.files(inproc, pattern = "*ndc_.*\\.parquet$", recursive = TRUE) 
ndcs <- open_dataset(paste0(inproc, list), format="parquet") |>
  filter(link_id %in% imoud_dates$link_id) |> collect()

# crosswalk
crosswalk <- readRDS(paste0("/home/data/moud/global/ndc_atc_crosswalk/","NDC_to_ATC_crosswalk_full.rds"))

# Code lists
benzos_atc <- read_csv(paste0(codes,"benzos_atc.csv"))
antidep_atc <- read_csv(paste0(codes,"antidep_atc.csv"))
antipsych_atc <- read_csv(paste0(codes,"antipsych_atc.csv"))
stims_atc <- read_csv(paste0(codes,"stims_atc.csv"))
clon_atc <- read_csv(paste0(codes,"clon_atc.csv"))
gaba_atc <- read_csv(paste0(codes,"gaba_atc.csv"))
ntxoral_ndc <- read_csv(paste0(codes,"ntxoral_ndc.csv"))

#################
# Processing
#################

filtercodes <- function(codes){
  codelist <- str_c(codes$code, collapse="|")
  
   # Find NDC codes in crosswalk
   ndclist <- crosswalk[sapply(crosswalk$atc, function(x) any(grepl(codelist, x))),]
   #test <- crosswalk |> filter(str_detect(atc,codelist)) #works but produces a warning
   
   # Merge into ndc data
   dat <- ndcs |>
     filter(NDC %in% ndclist$NDC) |>
     select(link_id,SRVC_BGN_DT,SRVC_END_DT)
   
   # Assess in lookback window
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

benz_dat <- filtercodes(benzos_atc) |> rename(med_benzos=variable,
                                              med_benzosalt=variable_alt)
ad_dat <- filtercodes(antidep_atc) |> rename(med_antidep=variable,
                                             med_antidepalt=variable_alt)
as_dat <- filtercodes(antipsych_atc) |> rename(med_antipsych=variable,
                                               med_antipsychalt=variable_alt)
stim_dat <- filtercodes(stims_atc) |> rename(med_stims=variable,
                                             med_stimsalt=variable_alt)
clon_dat <- filtercodes(clon_atc) |> rename(med_clon=variable,
                                            med_clonalt=variable_alt)
gaba_dat <- filtercodes(gaba_atc) |> rename(med_gaba=variable,
                                            med_gabaalt=variable_alt)

# Oral naltrexone -separately since using ndc
justntx_ndcdat <- ndcs |>
  filter(NDC %in% ntxoral_ndc$code)

setDT(justntx_ndcdat)
setkey(justntx_ndcdat,link_id,SRVC_BGN_DT,SRVC_END_DT) 

withntxndc <-  foverlaps(imoud_dates, justntx_ndcdat, 
                     by.x = c("link_id", "enroll_start", "mouddate"),
                     by.y = c("link_id", "SRVC_BGN_DT", "SRVC_END_DT"),
                     type = "any", mult = "all", nomatch=NULL)[,`:=` (med_ntxoral = 1,
                                                                      med_ntxoralalt = case_when(SRVC_END_DT>enrollalt_start ~ 1,
                                                                                               .default = 0))
                     ][,.(med_ntxoral=max(med_ntxoral),
                          med_ntxoralalt=max(med_ntxoralalt)), by=.(link_id,mouddate)]

ntx_dat <- imoud_dates |>
  select(link_id,mouddate) |>
  left_join(withntxndc, by=c("link_id","mouddate")) |>
  mutate(med_ntxoral = ifelse(is.na(med_ntxoral),0,med_ntxoral),
         med_ntxoralalt = ifelse(is.na(med_ntxoralalt),0,med_ntxoralalt))

# Combine
datlist <- list(benz_dat,ad_dat,as_dat,stim_dat,clon_dat,gaba_dat,ntx_dat)

base_meds <- reduce(datlist, ~left_join(.x,.y), by=c("link_id","mouddate"))

#################
# Save output
#################

saveRDS(base_meds,paste0(outpath,"base_meds.rds"))
print(paste0(nrow(base_meds),"in base_meds"))
toc()