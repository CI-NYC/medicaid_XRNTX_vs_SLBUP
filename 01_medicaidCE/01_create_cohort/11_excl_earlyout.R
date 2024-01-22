############################################
############################################
# Identify outcomes within first 14 days
# Author: Rachael Ross
#
# Output: excl_earlyout and also show fu_nontrt
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
# # outpath <- "/home/rr3551/moudr01/data/intermediate/"
# 
# #AWS/sample
# inraw <- "/home/rr3551/moudr01/data/sample/"
# inproc <- "/home/rr3551/moudr01/data/sample/processed/"
# outpath <- "/home/rr3551/moudr01/data/sample/intermediate/"
# 
# #################
# # Defined windows
# #################
# 
# assumptions <- read.csv(paste0(codes,"assumptions.csv"))
# 
# washoutmo <- assumptions[1,3] #months in washout
# mindayscover <- assumptions[2,3] #minimum days with coverage in a month to be considered enrolled

#################
# Load files
#################
tic()

# Code lists
overdose_dx <- read_csv(paste0(codes,"overdose_dx.csv"))

# Table of unique MOUD initiate dates
list <- list.files(outpath, pattern = "*imoud_dates*", recursive = TRUE) 
imoud_dates <- open_dataset(paste0(outpath, list), format="parquet") |> 
  select(-moud) |>
  collect() |>
  distinct() |>
  mutate(fudt_studyend = update(today(),month=12,year=2019,day=31))
setDT(imoud_dates)  

# Death date
filelist <- list.files(inraw, pattern = "*base.*\\.parquet$", recursive = TRUE) 
debase <- open_dataset(paste0(inraw, filelist), format="parquet") |>
  mutate(link_id = ifelse(grepl("^\\s*$", BENE_ID)==TRUE|is.na(BENE_ID),
                          paste0(MSIS_ID,STATE_CD),
                          paste0(BENE_ID,STATE_CD))) |>
  select(link_id,DEATH_DT) |>
  filter(link_id %in% imoud_dates$link_id) |>
  distinct() |>
  collect() 
  

# Enrollment monthly data
list <- list.files(inproc, pattern = "*enroll_*", recursive = TRUE) 
enroll_df <- open_dataset(paste0(inproc, list), format="parquet") |> 
  filter(link_id %in% imoud_dates$link_id) 

# # Enrollment date data - decided not to do this because it nearly exactly lines  up with the day of the month/days of coverage
# # i.e., if only covered 5 days, last day was month/5/year
# filelist <- list.files(inraw, pattern = "*dates.*\\.parquet$", recursive = TRUE) 
# dates <- open_dataset(paste0(inraw, filelist), format="parquet") |>
#   mutate(link_id = ifelse(grepl("^\\s*$", BENE_ID)==TRUE|is.na(BENE_ID),
#                           paste0(MSIS_ID,STATE_CD),
#                           paste0(BENE_ID,STATE_CD))) |>
#   filter(link_id %in% imoud_dates$link_id) |>
#   select(link_id,ENRLMT_END_DT,RFRNC_YR) |>
#   distinct() |>
#   collect() |>
#   mutate(MONTH = month(ENRLMT_END_DT), DAY = day(ENRLMT_END_DT))

# Diagnosis codes
list <- list.files(inproc, pattern = "*dx_.*\\.parquet$", recursive = TRUE) 
dxs <- open_dataset(paste0(inproc, list), format="parquet") |>
  filter(link_id %in% imoud_dates$link_id) |> 
  filter(DGNS_CD %in% overdose_dx$code) |>
  collect() 


#################
# Processing
#################

### Enrollment
# Add a date to enroll_monthly, also add flags for enrolled and eligible (enrolled, full benefits, nodual)
enroll_withdt <- enroll_df |>
  collect() |>
  mutate(enrolldt=update(today(),month=MONTH,year=YEAR,day=1),
         enrolled = ifelse(is.na(MDCD_ENRLMT_DAYS),0,
                           ifelse(MDCD_ENRLMT_DAYS<mindayscover,0,1)),
         eligible = ifelse(enrolled==0,0,
                           ifelse(nodual_flag %in% c(0,NA),0,
                                  ifelse(fullbnfts_flag %in% c(0,NA),0,1))),
         disenroll = ifelse(eligible==0 & (enrolled==0|fullbnfts_flag %in% c(0,NA)),1,0)) 

uneligmo <- enroll_withdt |> filter(eligible != 1) |>  #Month that are ineligible
  mutate(disenrolldt = case_when(fullbnfts_flag == 0 ~ enrolldt - 1,
                          nodual_flag == 0 ~ enrolldt - 1,
                          MDCD_ENRLMT_DAYS == 0 ~ enrolldt - 1,
                          MDCD_ENRLMT_DAYS<mindayscover ~ update(enrolldt,day=(MDCD_ENRLMT_DAYS + 1)),
                          .default = enrolldt - 1),
         dt2 = disenrolldt)

setDT(uneligmo) 
setkey(uneligmo,link_id,disenrolldt,dt2) 
enrollfu <-  foverlaps(imoud_dates, uneligmo, 
                   by.x = c("link_id", "mouddate", "fudt_studyend"),
                   by.y = c("link_id", "disenrolldt", "dt2"),
                   type = "any", mult = "all", nomatch=NULL)[, .(fudt_disenroll = min(disenrolldt)), 
                                                             by=.(link_id,mouddate)]



### Death data
setDT(debase)  
deathfu <-  debase[, .(fudt_death = min(DEATH_DT)),
                    by=.(link_id)]

### Overdose
setDT(dxs)
setkey(dxs,link_id,SRVC_BGN_DT,SRVC_END_DT) 

withdx <-  foverlaps(imoud_dates, dxs, 
                     by.x = c("link_id", "mouddate", "fudt_studyend"),
                     by.y = c("link_id", "SRVC_BGN_DT", "SRVC_END_DT"),
                     type = "any", mult = "all", nomatch=NULL)

overdosefu <- withdx[SRVC_BGN_DT>mouddate, #only services dates that occur after mouddate
                      .(fudt_overdose = min(SRVC_BGN_DT)), #which date to use here?
                      by=.(link_id,mouddate)] 

### Merge
with_fudates <- imoud_dates |>
  left_join(enrollfu, by=c("link_id","mouddate")) |>
  left_join(deathfu, by=c("link_id")) |>
  left_join(overdosefu, by=c("link_id","mouddate")) |>
  mutate(fudays_studyend = fudt_studyend - mouddate,
         fudays_death = fudt_death - mouddate,
         fudays_disenroll = fudt_disenroll - mouddate,
         fudays_overdose = fudt_overdose - mouddate) |>
  mutate(excl_studyend = case_when(fudays_studyend <= earlywindow ~ 1, .default=0),
         excl_earlydeath = case_when(fudays_death <= earlywindow ~ 1, .default=0),
         excl_earlydisenroll = case_when(fudays_disenroll <= earlywindow ~ 1, .default=0),
         excl_earlyoverdose = case_when(fudays_overdose <= earlywindow ~ 1, .default=0))

forexcl <- with_fudates |>
  select(link_id,mouddate,starts_with("excl_"))

forfu <- with_fudates |>
  select(link_id,mouddate,starts_with("fu"))


#################
# Save output
#################

write_parquet(forexcl,paste0(outpath,"excl_earlyout.parquet"))
saveRDS(forfu,paste0(outpath,"fu_nontrt.rds"))
print(paste0(nrow(forexcl),"in excl_earlyout"))
print(paste0(nrow(forfu),"in fu_nontrt"))
toc()