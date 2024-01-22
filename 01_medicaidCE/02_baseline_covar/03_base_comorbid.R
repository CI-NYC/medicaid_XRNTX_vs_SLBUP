############################################
############################################
# Create baseline comorbid non-psychiatric conditions variables
# Author: Rachael Ross
#
# Output: base_comorbid 
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

# Demographic and enrollment files
list <- list.files(inraw, pattern = "*base.*\\.parquet$", recursive = TRUE) 
debase <- open_dataset(paste0(inraw, list), format="parquet", partition = "year") |>
  mutate(link_id = ifelse(grepl("^\\s*$", BENE_ID)==TRUE|is.na(BENE_ID),
                          paste0(MSIS_ID,STATE_CD),
                          paste0(BENE_ID,STATE_CD))) |>
  filter(link_id %in% imoud_dates$link_id) |>
  mutate(YEAR=as.numeric(RFRNC_YR)) |>
  select(link_id,YEAR,starts_with("ELGBLTY_GRP_CD"))

# Diagnosis codes
list <- list.files(inproc, pattern = "*dx_.*\\.parquet$", recursive = TRUE) 
dxs <- open_dataset(paste0(inproc, list), format="parquet") |>
  filter(link_id %in% imoud_dates$link_id) #|> collect()

# Code lists
chronic_dx <- read_csv(paste0(codes,"chronic_dx.csv"))
disability_elig <- read_csv(paste0(codes,"disability_elig.csv"))


#################
# Processing
#################

### Chronic pain
setDT(imoud_dates) 

forpain <- dxs |>
  filter(DGNS_CD %in% chronic_dx$code) |> collect() |> as.data.table()

# #If list includes parent codes
# codelist <- str_c(chronic_dx$code, collapse="|")
#   
# forpain <- dxs |> 
#   filter(str_detect(DGNS_CD,codelist))
  
setkey(forpain,link_id,SRVC_BGN_DT,SRVC_END_DT) 
  
withdx <-  foverlaps(imoud_dates, forpain, 
                       by.x = c("link_id", "enroll_start", "mouddate"),
                       by.y = c("link_id", "SRVC_BGN_DT", "SRVC_END_DT"),
                       type = "any", mult = "all", nomatch=NULL)[,.(link_id,mouddate,SRVC_BGN_DT,DGNS_CD)]

withcat <- withdx |>
  inner_join(chronic_dx |> select(code,condition), by=c("DGNS_CD"="code")) |>
  select(link_id,mouddate,condition,SRVC_BGN_DT) |>
  distinct() |>
  as.data.table()

# Flag codes >=90 days apart
chronicpn <- unique(withcat[,com_chronicpn := 1*(SRVC_BGN_DT - min(SRVC_BGN_DT) >= 90), 
        by=.(link_id,mouddate,condition)][com_chronicpn==1][,.(link_id,mouddate,com_chronicpn)])


### disability - cannot do this with just 90 days lookback
disability_df <- debase |>
  collect() |>
  pivot_longer(cols=starts_with("ELGBLTY_GRP_CD"),
               names_to = c("MONTH"),
               names_prefix = "ELGBLTY_GRP_CD_",
               values_to = "ELGBLTY_GRP_CD") |>
  filter(ELGBLTY_GRP_CD %in% disability_elig$code) |>
  mutate(MONTH = as.numeric(factor(MONTH)),
         dt=update(today(),month=MONTH,year=YEAR,day=1),dt2=dt) |> as.data.table()

# Assess in washout
setkey(disability_df,link_id,dt,dt2) 

disability <-  foverlaps(imoud_dates, disability_df, 
                       by.x = c("link_id", "enroll_start", "mouddate"),
                       by.y = c("link_id", "dt", "dt2"),
                       type = "any", mult = "all", nomatch=NULL)[,`:=` (com_disability = 1,
                                                                        com_disabilityalt = case_when(dt>enrollalt_start ~ 1,
                                                                                                 .default = 0))
                       ][,.(com_disability=max(com_disability),
                            com_disabilityalt=max(com_disabilityalt)), by=.(link_id,mouddate)]


# Combine variables for output
base_comorbid <- imoud_dates |>
  select(link_id,mouddate) |>
  left_join(disability, by=c("link_id","mouddate")) |>
  left_join(chronicpn, by=c("link_id","mouddate")) |>
  mutate(com_disability = ifelse(is.na(com_disability),0,com_disability),
         com_disabilityalt = ifelse(is.na(com_disabilityalt),0,com_disabilityalt),
         com_chronicpn = ifelse(is.na(com_chronicpn),0,com_chronicpn))

#################
# Save output
#################

saveRDS(base_comorbid,paste0(outpath,"base_comorbid.rds"))
print(paste0(nrow(base_comorbid),"in base_comorbid"))
toc()