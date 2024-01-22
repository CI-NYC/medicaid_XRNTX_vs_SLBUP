############################################
############################################
# Create flag for exclusion: Pregnancy based on eligibility codes
# Author: Rachael Ross
#
# Output: excl_preg = file with flag for pregnancy eligibility for medicaid
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

# Demographic and enrollment files
filelist <- list.files(inraw, pattern = "*base.*\\.parquet$", recursive = TRUE) 
debase <- open_dataset(paste0(inraw, filelist), format="parquet", partition = "year") |>
  mutate(link_id = ifelse(grepl("^\\s*$", BENE_ID)==TRUE|is.na(BENE_ID),
                          paste0(MSIS_ID,STATE_CD),
                          paste0(BENE_ID,STATE_CD))) |>
  filter(link_id %in% imoud_dates$link_id) |>
  mutate(YEAR=as.numeric(RFRNC_YR)) |>
  select(link_id,YEAR,starts_with("ELGBLTY_GRP_CD"))


# Pregnancy code list
preg_elig <- read_csv(paste0(codes,"preg_elig.csv"))

#################
# Processing
#################

# Make eligibility variables long, filter on pregnancy codes
preg <- debase  |>
  collect() |>
  pivot_longer(cols=starts_with("ELGBLTY_GRP_CD"),
               names_to = c("MONTH"),
               names_prefix = "ELGBLTY_GRP_CD_",
               values_to = "ELGBLTY_GRP_CD") |>
  filter(ELGBLTY_GRP_CD %in% preg_elig$code) |>
  mutate(MONTH = as.numeric(factor(MONTH)),
         dt=update(today(),month=MONTH,year=YEAR,day=1))

# Merge to assess washout in window
excl_preg <- sqldf("SELECT distinct a.link_id, a.mouddate,
              case when b.link_id is NULL then 0 else 1 end as excl_preg
              FROM imoud_dates as a left join preg as b on 
              a.link_id=b.link_id and b.dt >= a.enroll_start and b.dt <= a.mouddate   
              order by a.link_id, a.mouddate")

excl_preg_alt <- sqldf("SELECT distinct a.link_id, a.mouddate,
              case when b.link_id is NULL then 0 else 1 end as excl_preg_alt
              FROM imoud_dates as a left join preg as b on 
              a.link_id=b.link_id and b.dt >= a.enrollalt_start and b.dt <= a.mouddate   
              order by a.link_id, a.mouddate")

setDT(excl_preg)
setDT(excl_preg_alt)
excl <- merge(excl_preg,excl_preg_alt)

#################
# Save output
#################

write_parquet(excl,paste0(outpath,"excl_preg.parquet"))
print(paste0(nrow(excl),"in excl_preg"))
toc()