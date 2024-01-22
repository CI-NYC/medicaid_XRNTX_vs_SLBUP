############################################
############################################
# Identify individuals and dates with OUD diagnosis
# Author: Rachael Ross
#
# Output: oud_df = all oud diagnosis unique by link_id and date
############################################
############################################

# #################
# # Libraries
# #################
# library(arrow)
# library(tidyverse)
# library(lubridate)
# library(tictoc)
# library(sqldf)
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
# Data
list <- list.files(inproc, pattern = "*dx_.*\\.parquet$", recursive = TRUE) 
dxs <- open_dataset(paste0(inproc, list), format="parquet")

# Code list
oud_dx <- read_csv(paste0(codes,"oud_dx.csv"))


#################
# Extract OUD dxs and dates
#################
# tic()
# dxs_table <- collect(dxs)
# oud_df <- sqldf("SELECT distinct link_id, YEAR, 
#                 SRVC_BGN_DT, SRVC_END_DT, 1 as oud_dx
#                 FROM dxs_table as a inner join oud_dx as b on a.DGNS_CD=b.code")
# toc()

oud_df <- dxs |>
  filter(DGNS_CD %in% oud_dx$code) |>
  select(link_id, SRVC_BGN_DT, SRVC_END_DT)  |>
  distinct() |>
  mutate(oud_dx=1) |>
  collect() 

#################
# Save output
#################

write_parquet(oud_df,paste0(outpath,"oud_dx.parquet"))
print(paste0(nrow(oud_df),"in oud_df"))
toc()