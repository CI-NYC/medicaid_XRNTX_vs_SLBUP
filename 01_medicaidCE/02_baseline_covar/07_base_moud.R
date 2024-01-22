############################################
############################################
# Create variables describing initiation
# Author: Rachael Ross
#
# Output: base_moud
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
# #AWS/sample
# inraw <- "/home/rr3551/moudr01/data/sample/"
# inproc <- "/home/rr3551/moudr01/data/sample/processed/"
# outpath <- "/home/rr3551/moudr01/data/sample/01_medicaidCE/intermediate/"

#################
# Load files
#################
tic()

# Load initial fills with dates
list <- list.files(outpath, pattern = "*imoud_dates*", recursive = TRUE) 
imoud_dates <- open_dataset(paste0(outpath, list), format="parquet") |> 
  select(link_id,mouddate,moud) |>
  distinct() |>
  collect() |> as.data.table()

# fills
anymoud <- open_dataset(paste0(outpath, "anymoud.parquet"), format="parquet") |>
  filter(link_id %in% imoud_dates$link_id) |> 
  mutate(source = ifelse(is.na(ed),source,"ed")) |>
  collect() |> as.data.table()

# moud_px <- open_dataset(paste0(outpath, "moud_px.parquet"), format="parquet") |>
#   filter(link_id %in% imoud_dates$link_id) |>
#   collect() |> as.data.table()
# 
# moud_ndc <- open_dataset(paste0(outpath, "moud_ndc.parquet"), format="parquet") |>
#   filter(link_id %in% imoud_dates$link_id) |>
#   collect() |> as.data.table()
# 
# # ed
# list <- list.files(inproc, pattern = "*ed_.*\\.parquet$", recursive = TRUE) 
# ed <- open_dataset(paste0(inproc, list), format="parquet") |>
#   filter(link_id %in% imoud_dates$link_id)  |>
#   select(link_id, CLM_ID) |>
#   distinct() |>
#   collect() |> 
#   mutate(ed=1) |>
#   as.data.table()

#################
# Processing
#################


summarized <- rbind(anymoud[initiate==1, .(days=ifelse(moud=="ntx",ntxinjlength,sum(days)),
                                                       cnt=.N), by=.(link_id,mouddate,moud,source,px)],
                    anymoud[initiate==1, .(days=ifelse(moud=="ntx",ntxinjlength,sum(days)),
                                                  cnt=.N), by=.(link_id,mouddate,moud,px)][,source:= "tot"]) |>
  mutate(px = ifelse(px==1,"px","ndc"))

wide <- dcast(melt(summarized, id.vars=c("link_id","mouddate","moud","source","px")), 
                  link_id+mouddate+moud~px+source+variable) |>
  mutate(mutate(across(ends_with("cnt")|contains("tot"), ~replace_na(.x, 0))),
         tot_days = px_tot_days + ndc_tot_days,
         miss_days = ifelse(tot_days <1,1,0),
         both = ifelse(px_tot_cnt>0 & ndc_tot_cnt>0,1,0))

names(wide)[4:ncol(wide)] <- paste0("moud_",names(wide)[4:ncol(wide)])

# 
# ### Methadone
# mth <- rbind(anymoud[moud=="mth" & initiate==1, .(days=sum(days),
#                                            cnt=.N), by=.(link_id,mouddate,moud,source,px)],
#              anymoud[moud=="mth" & initiate==1, .(days=sum(days),
#                                                   cnt=.N), by=.(link_id,mouddate,moud,px)][,source:= "tot"]) |>
#   mutate(px = ifelse(px==1,"px","ndc"))
# 
# mth_wide <- dcast(melt(mth, id.vars=c("link_id","mouddate","moud","source","px")), 
#                   link_id+mouddate+moud~px+source+variable) 
# 
# ### NTX
# ntx <- rbind(anymoud[moud=="ntx" & initiate==1, .(days=30,
#                                                   cnt=.N), by=.(link_id,mouddate,moud,source,px)],
#              anymoud[moud=="ntx" & initiate==1, .(days=30,
#                                                   cnt=.N), by=.(link_id,mouddate,moud,px)][,source:= "tot"]) |>
#   mutate(px = ifelse(px==1,"px","ndc"))
# 
# ntx_wide <- dcast(melt(ntx, id.vars=c("link_id","mouddate","moud","source","px")), 
#                   link_id+mouddate+moud~px+source+variable) 
# 
# ### Bup
# bup <- rbind(anymoud[moud=="bup" & initiate==1, .(days=30,
#                                                   cnt=.N), by=.(link_id,mouddate,moud,source,px)],
#              anymoud[moud=="bup" & initiate==1, .(days=30,
#                                                   cnt=.N), by=.(link_id,mouddate,moud,px)][,source:= "tot"]) |>
#   mutate(px = ifelse(px==1,"px","ndc"))
# 
# ntx_wide <- dcast(melt(ntx, id.vars=c("link_id","mouddate","moud","source","px")), 
#                   link_id+mouddate+moud~px+source+variable) 

# # Sum days and count codes, by date and claim and source
# px_sum1 <- copy(moudpx_wed)[moud.x==moud.y, .(days=sum(days),
#                                            cnt = .N), by=.(link_id,mouddate,moud.x,source,CLM_ID)][,`:=` (moud = moud.x,
#                                                                                                           moud.x = NULL)]                                                                                              
# # Sum again and get max, by date
# px_sum2 <- copy(px_sum1)[, .(sdays=sum(days),
#                        mdays = max(days),
#                        cnt = sum(cnt)), by=.(link_id,mouddate,moud,source)]
# 
# # Make wide
# px_sum <- dcast(melt(px_sum2, id.vars=c("link_id","mouddate","moud","source")), 
#                 link_id+mouddate+moud~"moud_px"+source+variable) |>
#   mutate(across(where(is.numeric), ~replace_na(.x, 0)),
#          tsdays = ip_sdays + os_sdays + ed_sdays,
#          tmdays = ip_mdays + os_mdays + ed_mdays,
#          tcnt = ip_cnt + os_cnt + ed_cnt)
# names(px_sum)[4:ncol(px_sum)] <- paste0(names(px_sum)[4:ncol(px_sum)])
# 

# px_summary <- copy(moudpx_wed)[moud.x==moud.y, .(moud_pxdays = sum(days),
#                                                moud_pxip = max(ifelse(source=="ip",1,0)),
#                                                moud_pxos = max(ifelse(source=="os",1,0)),
#                                                moud_pxed = max(ifelse(source=="ed",1,0)),
#                                                moud_pxcnt = .N), by=.(link_id,mouddate,moud.x)][,`:=` (moud = moud.x,
#                                                                                                        moud.x = NULL)]

# 
# ### Fills
# moud_ndc_ <- merge(imoud_dates,moud_ndc,
#                    by.x=c("link_id","mouddate"),
#                    by.y=c("link_id","RX_FILL_DT"))
# 
# # Add in ED flag
# moudndc_wed <- moud_ndc_ |>
#   left_join(ed, by=c("link_id","CLM_ID")) |>
#   mutate(source = case_when(source=="os" & ed==1 ~ "ed",
#                             .default = source)) #could remove ndcs here that have px for same drug on same clm_id
# 
# # Sum days and count codes, by date and claim and source
# ndc_sum1 <- copy(moudndc_wed)[moud.x==moud.y, .(days=sum(DAYS_SUPPLY),
#                                                 cnt = .N), by=.(link_id,mouddate,moud.x,source,CLM_ID)
#                               ][,`:=` (moud = moud.x,
#                                        moud.x = NULL)]
# # Sum again and get max, by date
# ndc_sum2 <- copy(ndc_sum1)[, .(sdays=sum(days),
#                              mdays = max(days),
#                              cnt = sum(cnt)), by=.(link_id,mouddate,moud,source)]
# 
# # Make wide
# ndc_sum <- dcast(melt(ndc_sum2, id.vars=c("link_id","mouddate","moud","source")), 
#                 link_id+mouddate+moud~source+variable) |>
#   mutate(across(where(is.numeric), ~replace_na(.x, 0)),
#          tsdays = ip_sdays + os_sdays + ed_sdays + rx_sdays,
#          tmdays = ip_mdays + os_mdays + ed_mdays + rx_mdays,
#          tcnt = ip_cnt + os_cnt + ed_cnt + rx_cnt)
# names(ndc_sum)[4:ncol(ndc_sum)] <- paste0("moud_ndc",names(ndc_sum)[4:ncol(ndc_sum)])

# ndc_summary <- copy(moudndc_wed)[moud.x==moud.y, .(moud_ndcdays = sum(DAYS_SUPPLY,na.rm = TRUE),
#                                  moud_ndcip = max(ifelse(source=="ip",1,0)),
#                                  moud_ndcos = max(ifelse(source=="os",1,0)),
#                                  moud_ndcrx = max(ifelse(source=="rx",1,0)),
#                                  moud_ndced = max(ifelse(source=="ed",1,0)),
#                                  moud_ndcnt = .N), by=.(link_id,mouddate,moud.x)][,`:=` (moud = moud.x,
#                                                                                         moud.x = NULL)]

# base_moud <- imoud_dates |>
#   select(link_id,mouddate,moud) |>
#   left_join(px_sum, by=c("link_id","mouddate","moud")) |>
#   left_join(ndc_sum, by=c("link_id","mouddate","moud")) |>
#   mutate(across(where(is.numeric), ~replace_na(.x, 0))) |>
#   mutate(moud_pxdays = ifelse(moud=="ntx" & moud_pxdays >30,30,moud_pxdays),
#          moud_ndcdays = ifelse(moud=="ntx" & moud_ndcdays >30,30,moud_ndcdays),
#          moud_totdays = moud_pxdays + moud_ndcdays) 


#################
# Save output
#################

saveRDS(wide,paste0(outpath,"base_moud.rds"))
print(paste0(nrow(wide),"in base_moud"))
toc()
