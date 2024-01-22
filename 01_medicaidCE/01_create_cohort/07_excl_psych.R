############################################
############################################
# Create flag for exclusion: Psychosis diagnosis in washout
# Author: Rachael Ross
#
# Output: excl_preg = file with flag for psychosis diagnosis in washout
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
# # #AWS/sample
# inraw <- "/home/rr3551/moudr01/data/sample/"
# inproc <- "/home/rr3551/moudr01/data/sample/processed/"
# outpath <- "/home/rr3551/moudr01/data/sample/intermediate/"

#################
# Load files
#################
tic()
# Table of unique MOUD initiate dates
list <- list.files(outpath, pattern = "*imoud_dates*", recursive = TRUE) 
imoud_dates <- open_dataset(paste0(outpath, list), format="parquet") |> 
  select(-moud) |>
  collect() |>
  distinct()

# Diagnosis codes
list <- list.files(inproc, pattern = "*dx_.*\\.parquet$", recursive = TRUE) 
dxs <- open_dataset(paste0(inproc, list), format="parquet") |>
  filter(link_id %in% imoud_dates$link_id)

# Code list
psych_dx <- read_csv(paste0(codes,"psych_dx.csv"))


#################
# Processing
#################

psychdx_df <- dxs |>
  filter(DGNS_CD %in% psych_dx$code) |>
  select(link_id, SRVC_BGN_DT, SRVC_END_DT)  |>
  distinct() |>
  collect()

# # Assess in washout
# inwash <- sqldf("SELECT distinct a.link_id, a.mouddate, 
#               case when b.link_id is NULL then 0 else 1 end as excl_psychdx
#               FROM imoud_dates as a left join psychdx_df as b on 
#               a.link_id=b.link_id and b.SRVC_END_DT >= a.w_start and b.SRVC_BGN_DT <= a.mouddate   
#               order by a.link_id, a.mouddate")

# Merge to assess oud dx in window
setDT(imoud_dates)  
setDT(psychdx_df)
setkey(psychdx_df,link_id,SRVC_BGN_DT,SRVC_END_DT) 

withdx <-  foverlaps(imoud_dates, psychdx_df, 
                     by.x = c("link_id", "enroll_start", "mouddate"),
                     by.y = c("link_id", "SRVC_BGN_DT", "SRVC_END_DT"),
                     type = "any", mult = "first", nomatch=NULL)[,excl_psychdx := 1
                     ][,.(link_id,mouddate,excl_psychdx)]

withdx_alt <-  foverlaps(imoud_dates, psychdx_df, 
                     by.x = c("link_id", "enrollalt_start", "mouddate"),
                     by.y = c("link_id", "SRVC_BGN_DT", "SRVC_END_DT"),
                     type = "any", mult = "first", nomatch=NULL)[,excl_psychdx_alt := 1
                     ][,.(link_id,mouddate,excl_psychdx_alt)]

# Merge back into imoud and make flag  
excl_psychdx <- imoud_dates |>
  left_join(withdx, by=c("link_id","mouddate")) |>
  left_join(withdx_alt, by=c("link_id","mouddate")) |>
  mutate(excl_psychdx = ifelse(is.na(excl_psychdx),0,1),
         excl_psychdx_alt = ifelse(is.na(excl_psychdx_alt),0,1)) |>
  select(link_id,mouddate,excl_psychdx,excl_psychdx_alt)

#################
# Save output
#################

write_parquet(excl_psychdx,paste0(outpath,"excl_psychdx.parquet"))
print(paste0(nrow(excl_psychdx),"in excl_psychdx"))
toc()