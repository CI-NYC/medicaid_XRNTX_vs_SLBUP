############################################
############################################
# Create flag for exclusion: NO OUD diagnosis in washout window
# Author: Rachael Ross
#
# Output: excl_noouddx = file with flag for NO oud dx in washout window
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

# #################
# # Load files
# #################
# tic()
# # OUD diagnoses
# list <- list.files(outpath, pattern = "*imoud_dates*", recursive = TRUE) 
# imoud_dates <- open_dataset(paste0(outpath, list), format="parquet") |> 
#   select(-moud) |>
#   collect() |>
#   distinct()
# 
# # OUD diagnoses
# list <- list.files(outpath, pattern = "*oud_dx_*", recursive = TRUE)
# oud_df <- open_dataset(paste0(outpath, list), format="parquet") |> 
#   filter(link_id %in% imoud_dates$link_id) |>
#   collect()
# 
# #################
# # Processing
# #################
# 
# # Merge to assess oud dx in window
# washout <- sqldf("SELECT distinct a.link_id, a.mouddate, 
#               case when b.link_id is NULL then 1 else 0 end as excl_noouddx
#               FROM imoud_dates as a left join oud_df as b on 
#               a.link_id=b.link_id and b.SRVC_END_DT >= a.w_start and b.SRVC_BGN_DT <= a.mouddate")
# toc()

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

# OUD diagnoses
list <- list.files(outpath, pattern = "*oud_dx_*", recursive = TRUE)
oud_df <- open_dataset(paste0(outpath, list), format="parquet") |> 
  filter(link_id %in% imoud_dates$link_id) |>
  collect()

#################
# Processing
#################

# Merge to assess oud dx in window
setDT(imoud_dates)  
setDT(oud_df)
setkey(oud_df,link_id,SRVC_BGN_DT,SRVC_END_DT) 

withdx <-  foverlaps(imoud_dates, oud_df, 
            by.x = c("link_id", "enroll_start", "mouddate"),
            by.y = c("link_id", "SRVC_BGN_DT", "SRVC_END_DT"),
            type = "any", mult = "first", nomatch=NULL)[,excl_noouddx := 0
                                                        ][,.(link_id,mouddate,excl_noouddx)]

withdx_alt <-  foverlaps(imoud_dates, oud_df, 
                     by.x = c("link_id", "enrollalt_start", "mouddate"),
                     by.y = c("link_id", "SRVC_BGN_DT", "SRVC_END_DT"),
                     type = "any", mult = "first", nomatch=NULL)[,excl_noouddx_alt := 0
                     ][,.(link_id,mouddate,excl_noouddx_alt)]

# Merge back into imoud and make flag  
excl_noouddx <- imoud_dates |>
  left_join(withdx, by=c("link_id","mouddate")) |>
  left_join(withdx_alt, by=c("link_id","mouddate")) |>
  mutate(excl_noouddx = ifelse(is.na(excl_noouddx),1,0),
         excl_noouddx_alt = ifelse(is.na(excl_noouddx_alt),1,0)) |>
  select(link_id,mouddate,excl_noouddx,excl_noouddx_alt)


#################
# Save output
#################

write_parquet(excl_noouddx,paste0(outpath,"excl_noouddx.parquet"))
print(paste0(nrow(excl_noouddx),"in excl_noouddx"))
toc()
