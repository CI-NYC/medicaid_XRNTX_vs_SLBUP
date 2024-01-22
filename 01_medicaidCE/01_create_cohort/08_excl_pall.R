############################################
############################################
# Create flag for exclusion: long-term/palliative/hospice care in washout
# Author: Rachael Ross
#
# Output: excl_pall = file with flag for LT/palliative/hosp care in washout window
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
dxs <- open_dataset(paste0(inproc, list), format="parquet") |> rename(code=DGNS_CD) |>
  filter(link_id %in% imoud_dates$link_id)

# Procedure codes
list <- list.files(inproc, pattern = "*px_.*\\.parquet$", recursive = TRUE) 
pxs <- open_dataset(paste0(inproc, list), format="parquet") |> rename(code=PRCDR_CD) |>
  filter(link_id %in% imoud_dates$link_id)

# Revenue codes
list <- list.files(inproc, pattern = "*rev_.*\\.parquet$", recursive = TRUE) 
revs <- open_dataset(paste0(inproc, list), format="parquet") |> rename(code=REV_CNTR_CD) |>
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

# IP header files
list <- list.files(inraw, pattern = "*inpatient_header.*\\.parquet$", recursive = TRUE) 
iphead <- open_dataset(paste0(inraw, list), format="parquet", partition = "year") |>
  mutate(link_id = ifelse(grepl("^\\s*$", BENE_ID)==TRUE|is.na(BENE_ID),
                          paste0(MSIS_ID,STATE_CD),
                          paste0(BENE_ID,STATE_CD))) |>
  select(link_id,PTNT_DSCHRG_STUS_CD,SRVC_BGN_DT,SRVC_END_DT,ADMSN_DT,DSCHRG_DT,starts_with("PRCDR_CD_DT_")) |>
  rename(code=PTNT_DSCHRG_STUS_CD) |>
  filter(link_id %in% imoud_dates$link_id) |>
  collect() |>
  mutate(SRVC_BGN_DT=fifelse(is.na(SRVC_BGN_DT),SRVC_END_DT,SRVC_BGN_DT),
         maxdate = pmax(SRVC_BGN_DT,SRVC_END_DT,ADMSN_DT,DSCHRG_DT,
                        PRCDR_CD_DT_1,PRCDR_CD_DT_2,PRCDR_CD_DT_3,PRCDR_CD_DT_4,PRCDR_CD_DT_5,PRCDR_CD_DT_6,
                        na.rm = TRUE),
         SRVC_END_DT=fifelse(SRVC_BGN_DT>SRVC_END_DT,maxdate,SRVC_END_DT)) |>
  select(link_id,code,SRVC_BGN_DT,SRVC_END_DT)


# Code lists
dxlist <- read_csv(paste0(codes,"pall_dx.csv"))
eliglist <- read_csv(paste0(codes,"pall_elig.csv")) |> mutate(code=str_pad(code, 2, pad="0"))
hcpcslist <- read_csv(paste0(codes,"pall_hcpcs.csv"))
ipdslist <- read_csv(paste0(codes,"pall_ipds.csv"))
revlist <- read_csv(paste0(codes,"pall_rev.csv"))

#################
# Processing dx, px, rev & ip discharge codes
#################

setDT(imoud_dates) 

toassess <- function(dat,list){
  
  # Filter relevant codes
  df <- dat |>
    filter(code %in% list$code) |>
    select(link_id, SRVC_BGN_DT, SRVC_END_DT)  |>
    distinct() |>
    collect()
  
  # Merge to assess in washout window
  
  # sqldf("SELECT distinct a.link_id, a.mouddate,
  #             case when b.link_id is NULL then 0 else 1 end as excl_
  #             FROM imoud_dates as a left join df as b on
  #             a.link_id=b.link_id and b.SRVC_END_DT >= a.w_start and b.SRVC_BGN_DT <= a.mouddate
  #             order by a.link_id, a.mouddate")

  setDT(df)
  setkey(df,link_id,SRVC_BGN_DT,SRVC_END_DT)

  withdx <-  foverlaps(imoud_dates, df,
                       by.x = c("link_id", "enroll_start", "mouddate"),
                       by.y = c("link_id", "SRVC_BGN_DT", "SRVC_END_DT"),
                       type = "any", mult = "first", nomatch=NULL)[,excl_ := 1
                       ][,.(link_id,mouddate,excl_)]
  
  withdx_alt <-  foverlaps(imoud_dates, df,
                       by.x = c("link_id", "enrollalt_start", "mouddate"),
                       by.y = c("link_id", "SRVC_BGN_DT", "SRVC_END_DT"),
                       type = "any", mult = "first", nomatch=NULL)[,exclalt_ := 1
                       ][,.(link_id,mouddate,exclalt_)]

  # Merge back into imoud and make flag
  imoud_dates |>
    left_join(withdx, by=c("link_id","mouddate")) |>
    left_join(withdx_alt, by=c("link_id","mouddate")) |>
    mutate(excl_ = ifelse(is.na(excl_),0,excl_),
           exclalt_ = ifelse(is.na(exclalt_),0,exclalt_))
  
}

excl_palldx <- toassess(dxs, dxlist) |> rename(excl_palldx=excl_,
                                               excl_palldx_alt=exclalt_)
excl_pallrev <- toassess(revs, revlist) |> rename(excl_pallrev=excl_,
                                                  excl_pallrev_alt=exclalt_)
excl_pallpx <- toassess(pxs, hcpcslist) |> rename(excl_pallpx=excl_,
                                                  excl_pallpx_alt=exclalt_)
excl_pallipd <- toassess(iphead, ipdslist) |> rename(excl_pallipd=excl_,
                                                     excl_pallipd_alt=exclalt_)
                                                     

#################
# Processing eligibility codes
#################

pallelig_df <- debase |>
  collect() |>
  pivot_longer(cols=starts_with("ELGBLTY_GRP_CD"),
               names_to = c("MONTH"),
               names_prefix = "ELGBLTY_GRP_CD_",
               values_to = "ELGBLTY_GRP_CD") |>
  mutate(MONTH = as.numeric(factor(MONTH)),
         dt=update(today(),month=MONTH,year=YEAR,day=1),dt2=dt) |>
  filter(ELGBLTY_GRP_CD %in% eliglist$code) 

# Assess in washout
# excl_pallelig <- sqldf("SELECT distinct a.link_id, a.mouddate,
#               case when b.link_id is NULL then 0 else 1 end as excl_pallelig
#               FROM imoud_dates as a left join pallelig_df as b on 
#               a.link_id=b.link_id and b.dt >= a.w_start and b.dt <= a.mouddate   
#               order by a.link_id, a.mouddate")

setDT(pallelig_df)
setkey(pallelig_df,link_id,dt,dt2) 

withelig <-  foverlaps(imoud_dates, pallelig_df, 
                             by.x = c("link_id", "enroll_start", "mouddate"),
                             by.y = c("link_id", "dt", "dt2"),
                             type = "any", mult = "first", nomatch=NULL)[,excl_pallelig := 1
                             ][,.(link_id,mouddate,excl_pallelig)]

withelig_alt <-  foverlaps(imoud_dates, pallelig_df, 
                       by.x = c("link_id", "enrollalt_start", "mouddate"),
                       by.y = c("link_id", "dt", "dt2"),
                       type = "any", mult = "first", nomatch=NULL)[,excl_pallelig_alt := 1
                       ][,.(link_id,mouddate,excl_pallelig_alt)]

excl_pallelig <- imoud_dates |>
  left_join(withelig, by=c("link_id","mouddate")) |>
  left_join(withelig_alt, by=c("link_id","mouddate")) |>
  mutate(excl_pallelig = ifelse(is.na(excl_pallelig),0,excl_pallelig),
         excl_pallelig_alt = ifelse(is.na(excl_pallelig_alt),0,excl_pallelig_alt))

#################
# Merge
#################

merged <- excl_pallelig |>
  inner_join(excl_palldx, by=c("link_id","mouddate")) |>
  inner_join(excl_pallrev, by=c("link_id","mouddate")) |>
  inner_join(excl_pallpx, by=c("link_id","mouddate")) |>
  inner_join(excl_pallipd, by=c("link_id","mouddate")) |>
  mutate(excl_pall=ifelse(excl_pallelig==1|excl_palldx==1|
                              excl_pallrev==1|excl_pallpx==1|
                              excl_pallipd==1,1,0),
         excl_pall_alt=ifelse(excl_pallelig_alt==1|excl_palldx_alt==1|
                            excl_pallrev_alt==1|excl_pallpx_alt==1|
                            excl_pallipd_alt==1,1,0)) |>
  select(link_id,mouddate,excl_pall,excl_pall_alt)

#################
# Save output
#################

write_parquet(merged,paste0(outpath,"excl_pall.parquet"))
print(paste0(nrow(merged),"in excl_pall"))
toc()