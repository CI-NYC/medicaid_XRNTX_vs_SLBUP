############################################
############################################
# Create baseline oud characteristics
# Author: Rachael Ross
#
# Output: base_oud
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
# library(data.table)
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

# Diagnosis codes
list <- list.files(inproc, pattern = "*dx_.*\\.parquet$", recursive = TRUE) 
dxs <- open_dataset(paste0(inproc, list), format="parquet") |>
  filter(link_id %in% imoud_dates$link_id) 

# Procedure codes
list <- list.files(inproc, pattern = "*px_.*\\.parquet$", recursive = TRUE)
pxs <- open_dataset(paste0(inproc, list), format="parquet") |>
  filter(link_id %in% imoud_dates$link_id)

# Code lists
oud_dx <- read_csv(paste0(codes,"oud_dx.csv"))
overdose_dx <- read_csv(paste0(codes,"overdose_dx.csv"))
therapy_px <- read_csv(paste0(codes,"therapy_px.csv"))


#################
# Processing
#################

# Inpatient with primary dx of oud
ipoud <- dxs |>
  filter(source=="ip" & NUMBER==1 & DGNS_CD %in% oud_dx$code) |>
  select(link_id,SRVC_BGN_DT,SRVC_END_DT) |>
  distinct() |>
  collect()
setDT(ipoud)
setkey(ipoud,link_id,SRVC_BGN_DT,SRVC_END_DT)

# Overdose
overdose <- dxs |>
  filter(DGNS_CD %in% overdose_dx$code) |>
  select(link_id,SRVC_BGN_DT,SRVC_END_DT) |>
  distinct() |>
  collect()
setDT(overdose)
setkey(overdose,link_id,SRVC_BGN_DT,SRVC_END_DT)

# Therapy
therapy <- pxs |>
  filter(PRCDR_CD %in% therapy_px$code) |>
  select(link_id,PRCDR_CD_DT) |>
  mutate(dt2=PRCDR_CD_DT) |>
  distinct() |>
  collect()
setDT(therapy)
setkey(therapy,link_id,PRCDR_CD_DT,dt2)


# Filter above on washout
setDT(imoud_dates) 

filterinwash <- function(data,start,end){
  withclaim <-  foverlaps(imoud_dates, data, 
                          by.x = c("link_id", "enroll_start", "mouddate"),
                          by.y = c("link_id", start, end),
                          type = "any", mult = "all", nomatch=NULL)
  withclaim
}


# IP OUD, yes/no
ipoud_dat <- filterinwash(ipoud,"SRVC_BGN_DT","SRVC_END_DT")[,`:=` (oud_inpatient = 1,
                                                                    oud_inpatientalt = case_when(SRVC_END_DT>enrollalt_start ~ 1,
                                                                                                  .default = 0))
                                                             ][,.(oud_inpatient=max(oud_inpatient),
                                                                  oud_inpatientalt=max(oud_inpatientalt)), 
                                                               by=.(link_id,mouddate)]
# Any therapy, yes/no
therapy_dat <- filterinwash(therapy,"PRCDR_CD_DT","dt2")[,`:=` (oud_therapy = 1,
                                                                oud_therapyalt = case_when(PRCDR_CD_DT>enrollalt_start ~ 1,
                                                                                             .default = 0))
                                                         ][,.(oud_therapy=max(oud_therapy),
                                                              oud_therapyalt=max(oud_therapyalt)), 
                                                           by=.(link_id,mouddate)]

# Overdose
overdose_dat <- filterinwash(overdose,"SRVC_BGN_DT","SRVC_END_DT") |>
  select(link_id,mouddate,enrollalt_start,SRVC_END_DT) |>
  distinct() |>
  mutate(in30=ifelse(mouddate-SRVC_END_DT<=30,1,0),
         in60=ifelse(mouddate-SRVC_END_DT<=60,1,0),
         inalt=ifelse(SRVC_END_DT>enrollalt_start,1,0)) 

setDT(overdose_dat)
overdose_dat_ <- overdose_dat[, .(oud_overdose30 = max(in30),
                                  oud_overdose60 = max(in60),
                                  oud_overdosecnt = .N,
                                  oud_overdosecntalt = sum(inalt)), by=.(link_id,mouddate)
                              ][, `:=`(oud_overdose_cat = ifelse(oud_overdosecnt>1,2,oud_overdosecnt),
                                       oud_overdoseany = ifelse(oud_overdosecnt>0,1,0),
                                       oud_overdose_catalt = ifelse(oud_overdosecntalt>1,2,oud_overdosecntalt),
                                       oud_overdoseanyalt = ifelse(oud_overdosecntalt>0,1,0))]
                                       #oud_overdose_cat1 = ifelse(oud_overdosecnt==1,1,0),
                                       #oud_overdose_cat2 = ifelse(oud_overdosecnt==2,1,0),
                                       #oud_overdose_cat3 = ifelse(oud_overdosecnt>2,1,0))]

# Combine
base_oud <- imoud_dates |>
  select(link_id,mouddate) |>
  left_join(ipoud_dat, by=c("link_id","mouddate")) |>
  left_join(therapy_dat, by=c("link_id","mouddate")) |>
  left_join(overdose_dat_, by=c("link_id","mouddate")) |>
  mutate(across(where(is.numeric), ~replace_na(.x, 0)),
         oud_overdose_cat = factor(oud_overdose_cat,
                                   labels = c("0","1",">1")),
         oud_overdose_catalt = factor(oud_overdose_catalt,
                                   labels = c("0","1",">1")))


#################
# Save output
#################

saveRDS(base_oud,paste0(outpath,"base_oud.rds"))
print(paste0(nrow(base_oud),"in base_oud"))
toc()