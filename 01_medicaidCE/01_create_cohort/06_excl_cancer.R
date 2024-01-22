############################################
############################################
# Create flag for exclusion: Cancer diagnosis/elig in washout
# Author: Rachael Ross
#
# Output: excl_cancer = file with flag for cancer diagnosis/eligibility washout window
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

# Demographic and enrollment files
list <- list.files(inraw, pattern = "*base.*\\.parquet$", recursive = TRUE) 
debase <- open_dataset(paste0(inraw, list), format="parquet", partition = "year") |>
  mutate(link_id = ifelse(grepl("^\\s*$", BENE_ID)==TRUE|is.na(BENE_ID),
                          paste0(MSIS_ID,STATE_CD),
                          paste0(BENE_ID,STATE_CD))) |>
  filter(link_id %in% imoud_dates$link_id) |>
  mutate(YEAR=as.numeric(RFRNC_YR)) |>
  select(link_id,YEAR,starts_with("ELGBLTY_GRP_CD"))

# Cancer code lists
cancer_dx <- read_csv(paste0(codes,"cancer_dx.csv"))
cancer_elig <- read_csv(paste0(codes,"cancer_elig.csv")) #coming in as dbl but still merging fine below

#################
# Processing diagnosis codes
#################

# #Some codes are missing from list
# cancerdx_df <- dxs |>
#   filter(DGNS_CD %in% cancer_dx$code) |>
#   select(DGNS_CD) |>
#   distinct() |>
#   collect()

# cancerdx_df1 <- dxs |>
#   collect() |>
#   filter(startsWith(DGNS_CD,"C")|startsWith(DGNS_CD,"D0")) |>
#   select(DGNS_CD) |>
#   distinct()

cancerdx_df <- dxs |>
  filter(substr(DGNS_CD,1,1)=="C"|substr(DGNS_CD,1,2)=="D0") |>
  select(link_id, SRVC_BGN_DT, SRVC_END_DT)  |>
  distinct() |>
  collect()

# # Assess in washout
# cancerdx_inwash <- sqldf("SELECT distinct a.link_id, a.mouddate, 
#               case when b.link_id is NULL then 0 else 1 end as excl_cancerdx
#               FROM imoud_dates as a left join cancerdx_df as b on 
#               a.link_id=b.link_id and b.SRVC_END_DT >= a.w_start and b.SRVC_BGN_DT <= a.mouddate   
#               order by a.link_id, a.mouddate")


# Merge to assess oud dx in window
setDT(imoud_dates)  
setDT(cancerdx_df)
setkey(cancerdx_df,link_id,SRVC_BGN_DT,SRVC_END_DT) 

withdx <-  foverlaps(imoud_dates, cancerdx_df, 
                     by.x = c("link_id", "enroll_start", "mouddate"),
                     by.y = c("link_id", "SRVC_BGN_DT", "SRVC_END_DT"),
                     type = "any", mult = "first", nomatch=NULL)[,excl_cancerdx := 1
                     ][,.(link_id,mouddate,excl_cancerdx)]

withdx_alt <-  foverlaps(imoud_dates, cancerdx_df, 
                     by.x = c("link_id", "enrollalt_start", "mouddate"),
                     by.y = c("link_id", "SRVC_BGN_DT", "SRVC_END_DT"),
                     type = "any", mult = "first", nomatch=NULL)[,excl_cancerdx_alt := 1
                     ][,.(link_id,mouddate,excl_cancerdx_alt)]

# Merge back into imoud and make flag  
cancerdx_inwash <- imoud_dates |>
  left_join(withdx, by=c("link_id","mouddate")) |>
  left_join(withdx_alt, by=c("link_id","mouddate")) |>
  mutate(excl_cancerdx = ifelse(is.na(excl_cancerdx),0,1),
         excl_cancerdx_alt = ifelse(is.na(excl_cancerdx_alt),0,1)) 

#################
# Processing eligibility codes 
#################

cancerelig_df <- debase |>
  collect() |>
  pivot_longer(cols=starts_with("ELGBLTY_GRP_CD"),
               names_to = c("MONTH"),
               names_prefix = "ELGBLTY_GRP_CD_",
               values_to = "ELGBLTY_GRP_CD") |>
  mutate(MONTH = as.numeric(factor(MONTH)),
         dt=update(today(),month=MONTH,year=YEAR,day=1)) |>
  filter(ELGBLTY_GRP_CD %in% cancer_elig$code) 

# Assess in washout
cancerelig_inwash <- sqldf("SELECT distinct a.link_id, a.mouddate,
              case when b.link_id is NULL then 0 else 1 end as excl_cancerelig
              FROM imoud_dates as a left join cancerelig_df as b on 
              a.link_id=b.link_id and b.dt >= a.enroll_start and b.dt <= a.mouddate   
              order by a.link_id, a.mouddate")

cancerelig_inwash_alt <- sqldf("SELECT distinct a.link_id, a.mouddate,
              case when b.link_id is NULL then 0 else 1 end as excl_cancerelig_alt
              FROM imoud_dates as a left join cancerelig_df as b on 
              a.link_id=b.link_id and b.dt >= a.enrollalt_start and b.dt <= a.mouddate   
              order by a.link_id, a.mouddate")

#################
# Merge
#################

merged <- cancerelig_inwash |>
  inner_join(cancerelig_inwash_alt, by=c("link_id","mouddate")) |>
  inner_join(cancerdx_inwash, by=c("link_id","mouddate")) |>
  mutate(excl_cancer=ifelse(excl_cancerelig==1|excl_cancerdx==1,1,0),
         excl_cancer_alt=ifelse(excl_cancerelig_alt==1|excl_cancerdx_alt==1,1,0)) |>
  select(link_id,mouddate,excl_cancer,excl_cancer_alt)
  
#################
# Save output
#################

write_parquet(merged,paste0(outpath,"excl_cancer.parquet"))
print(paste0(nrow(merged),"in excl_cancer"))
toc()