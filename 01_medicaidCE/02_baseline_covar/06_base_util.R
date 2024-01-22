############################################
############################################
# Create baseline healthcare utilization variables
# Author: Rachael Ross
#
# Output: base_util
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
  distinct() |>
  as.data.table()

# claims
list <- list.files(inproc, pattern = "*dx_ip.*\\.parquet$", recursive = TRUE) 
ipclaims <- open_dataset(paste0(inproc, list), format="parquet") |>
  filter(link_id %in% imoud_dates$link_id) |> 
  select(link_id, CLM_ID, SRVC_BGN_DT, SRVC_END_DT,source) |>
  distinct() |>
  collect() |>
  as.data.table()

list <- list.files(inproc, pattern = "*dx_os.*\\.parquet$", recursive = TRUE) 
osclaims <- open_dataset(paste0(inproc, list), format="parquet") |>
  filter(link_id %in% imoud_dates$link_id) |> 
  select(link_id, CLM_ID, SRVC_BGN_DT, SRVC_END_DT,source,YEAR) |>
  distinct() |>
  collect() |>
  as.data.table()

# ED visits
list <- list.files(inproc, pattern = "*ed_.*\\.parquet$", recursive = TRUE) 
ed <- open_dataset(paste0(inproc, list), format="parquet") |>
  filter(link_id %in% imoud_dates$link_id)  |>
  select(link_id, CLM_ID) |>
  distinct() |>
  collect() |> 
  as.data.table()


#################
# Processing
#################

# In full data the following foverlap fails because data are too big
# Separate out os claims into ed
# Cut remaining os claims into subsections

osedclaims <- merge(osclaims,ed) 

setkey(ipclaims,link_id,SRVC_BGN_DT,SRVC_END_DT) 
setkey(osedclaims,link_id,SRVC_BGN_DT,SRVC_END_DT) 

# Split OS claims by year and remove ED visits
splitos <- function(year){
  split <- osclaims[YEAR==year]
  noed <- split |> anti_join(ed) |> select(-YEAR)
  setkey(noed,link_id,SRVC_BGN_DT,SRVC_END_DT) 
  noed
}

os16 <- splitos("2016") 
os17 <- splitos("2017")
os18 <- splitos("2018")
os19 <- splitos("2019")

# Function for utilization merges
utilfx <- function(data){
  withclaim <-  foverlaps(imoud_dates, data, 
                          by.x = c("link_id", "enroll_start", "mouddate"),
                          by.y = c("link_id", "SRVC_BGN_DT", "SRVC_END_DT"),
                          type = "any", mult = "all", nomatch=NULL)[,`:=` (cnt=1,
                                                                          cntalt=ifelse(SRVC_END_DT>enrollalt_start,1,0))]
  
  # Remove the ones on the same day as or overlapping with mouddate
  imoud_datesalt <- imoud_dates |> mutate(d2=mouddate)
  samedayclaim <- foverlaps(imoud_datesalt, data, 
                            by.x = c("link_id", "d2", "mouddate"),
                            by.y = c("link_id", "SRVC_BGN_DT", "SRVC_END_DT"),
                            type = "any", mult = "all", nomatch=NULL)
  
  # Claims excluding 14 day period immediately prior
  imoud_datesalt <- imoud_dates |> mutate(d2=(mouddate-days(15)))
  excl14 <- foverlaps(imoud_datesalt, data, 
                      by.x = c("link_id", "enroll_start", "d2"),
                      by.y = c("link_id", "SRVC_BGN_DT", "SRVC_END_DT"),
                      type = "any", mult = "all", nomatch=NULL)[,`:=` (cnt=1,
                                                                      cntalt=ifelse(SRVC_END_DT>enrollalt_start,1,0))]

  # Create summaries
  cnt <- withclaim[,.(cnt=sum(cnt),
                      cntalt=sum(cntalt)), by=.(link_id,mouddate)]
  rm_cnt <- samedayclaim[,.(remove=.N), by=.(link_id,mouddate)]
  cnt_excl14 <- excl14[,.(cntexcl14=sum(cnt),
                          cntexcl14alt=sum(cntalt)), by=.(link_id,mouddate)]

imoud_dates |>
  select(link_id,mouddate) |>
    left_join(cnt, by=c("link_id","mouddate")) |>
    left_join(rm_cnt, by=c("link_id","mouddate")) |>
    left_join(cnt_excl14, by=c("link_id","mouddate")) |>
    mutate(cnt = ifelse(is.na(cnt),0,
                        ifelse(is.na(remove), cnt, cnt-remove)),
           cntalt = ifelse(is.na(cntalt),0,
                        ifelse(is.na(remove), cntalt, cntalt-remove)),
           cntexcl14 = ifelse(is.na(cntexcl14),0,cntexcl14),
           cntexcl14alt = ifelse(is.na(cntexcl14alt),0,cntexcl14alt)) |>
    select(-remove)  
}

# Call function
util_osed <- utilfx(osedclaims) |> rename(util_edcnt=cnt,
                                          util_edcntalt=cntalt,
                                          util_edcntex14=cntexcl14,
                                          util_edcntex14alt=cntexcl14alt) |>
  mutate(util_edany = ifelse(util_edcnt>0,1,0),
         util_edanyalt = ifelse(util_edcntalt>0,1,0))
util_ip <- utilfx(ipclaims) |> rename(util_ipcnt=cnt,
                                      util_ipcntalt=cntalt,
                                      util_ipcntex14=cntexcl14,
                                      util_ipcntex14alt=cntexcl14alt) |>
  mutate(util_ipany = ifelse(util_ipcnt>0,1,0),
         util_ipanyalt = ifelse(util_ipcntalt>0,1,0))

util_os16 <- utilfx(os16)
util_os17 <- utilfx(os17)
util_os18 <- utilfx(os18)
util_os19 <- utilfx(os19)

util_os <- rbind(util_os16,
                 util_os17,
                 util_os18,
                 util_os19)[,.(util_oscnt=sum(cnt),
                               util_oscntalt=sum(cntalt),
                               util_oscntex14=sum(cntexcl14),
                               util_oscntex14alt=sum(cntexcl14alt)),by=.(link_id,mouddate)]

# Combine
base_util <- util_os |>
  left_join(util_ip, by=c("link_id","mouddate")) |>
  left_join(util_osed, by=c("link_id","mouddate")) 

#################
# Save output
#################

saveRDS(base_util,paste0(outpath,"base_util.rds"))
print(paste0(nrow(base_util),"in base_util"))
toc()