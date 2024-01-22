############################################
############################################
# Define MOUD treatment episodes
# Author: Rachael Ross
#
# Output: moudepisodes: file of start and end date of moud episodes
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

#################-
# Load files----
#################-
tic()

# Load initial fills with dates
list <- list.files(outpath, pattern = "*imoud_dates*", recursive = TRUE) 
imoud_dates <- open_dataset(paste0(outpath, list), format="parquet") |> 
  select(link_id) |>
  distinct() |> collect() 

# fills
anymoud <- open_dataset(paste0(outpath, "anymoud.parquet"), format="parquet") |>
  filter(link_id %in% imoud_dates$link_id)  |>
  collect() |> 
  rename(dt = mouddate) |>
  mutate(stock = case_when(source == "rx" ~ 1,
                           .default = 0)) |>
  select(link_id,dt,moud,route,stock,days) |>
  filter(moud %in% c("mth","ntx","bup")) |>
  as.data.table()


#################-
# Identify trt episodes----
#################-

#table(anymoud$fu_moud,anymoud$fu_route)
#test <- anymoud |> filter(fu_route=="injection" & fu_moud=="bup")

#inj ntx - starts on date, regardless of source (px or ndc)
#oral mth - starts on date, regardless of source (only px - ndc does not happen)
#inj bup - starts on date, regardless of source (px or ndc)
#imp bup - starts on date, regardless of source (px or ndc)
#oral bup - starts on date, regardless of source (px or ndc) - per 10/24/23 convo with kara - no stockpiling

# Get days by date
allfufills <- anymoud[,.(days=fcase(route=="injection", ntxinjlength,
                                    route=="implant", bupimplength,
                                    default = sum(days))), 
                      by=.(link_id,dt,moud,route)][order(link_id,moud,dt,days,route)]

# Function for identifying episodes without stockpiling
nostock <- function(fmoud,froute,grace){
  dts <- allfufills |>
    filter(moud==fmoud) |>
    filter(route %in% froute) |>
    mutate(end = dt + days - 1,
           endgap = end + grace) |>
    arrange(link_id,dt,end) |>
    mutate(lagged = lag(endgap, default = first(endgap)),
           course = cumsum(cummax(as.integer(lagged)) < dt), .by=link_id) |>
    data.table()

    
  dts[,.(startdt=min(dt),
               enddt=max(end),
               endgracedt = max(end) + grace),by=.(link_id,course)]    
}


ntx_episodes <- nostock("ntx",c("injection"),moudgrace) |> mutate(trt="ntx",
                                                                    initiate = 1)
mth_episodes <- nostock("mth",c("oral"),moudgrace) |> mutate(trt="mth",
                                                               initiate = 1)
bupxr_episodes <- nostock("bup",c("injection","implant"),moudgrace) |> mutate(trt="bup",
                                                                                initiate = 0)
bupo_episodes <- nostock("bup",c("oral"),moudgrace) |> mutate(trt="bup",
                                                                initiate = 1) 

# With alternative grace period
ntx_episodes_alt <- nostock("ntx",c("injection"),moudgrace_alt) |> mutate(trt="ntx",
                                                                  initiate = 1)
mth_episodes_alt <- nostock("mth",c("oral"),moudgrace_alt) |> mutate(trt="mth",
                                                             initiate = 1)
bupxr_episodes_alt <- nostock("bup",c("injection","implant"),moudgrace_alt) |> mutate(trt="bup",
                                                                              initiate = 0)
bupo_episodes_alt <- nostock("bup",c("oral"),moudgrace_alt) |> mutate(trt="bup",
                                                              initiate = 1) 

###-
## Combine episodes ----
###-


## Stacking individual initiate drugs (used in definition c)
indiv_episodes <- rbind(ntx_episodes,
                     mth_episodes, # at this time methadone is not included in cohorts
                     bupo_episodes,
                     bupxr_episodes)

## For combined episodes for definitions a and b

# Function for combining episodes
combineepi <- function(data){
  dts <-  data |>
    select(-course) |>
    arrange(link_id,startdt,enddt) |>
    mutate(lagged = lag(endgracedt, default = first(endgracedt)),
           course = cumsum(cummax(as.integer(lagged)) < startdt), .by=link_id) |>
    data.table()
  
  
  dts[,.(startdt=min(startdt),
         enddt=max(enddt),
         endgracedt = max(endgracedt)),by=.(link_id,course)]    
}

# Use fx to combine bup episodes
bup_episodes <- setDT(rbind(bupxr_episodes,
                            bupo_episodes))[order(link_id,startdt,enddt,-initiate)] |> 
  combineepi()

# Use fx to combine all moud episodes
any_episodes <- setDT(rbind(ntx_episodes,
                            mth_episodes,
                            bupxr_episodes,
                            bupo_episodes))[order(link_id,startdt,enddt,-initiate)] |>
  combineepi()

# Use fx to combine all moud episodes with alternate grace period
any_episodes_alt <- setDT(rbind(ntx_episodes_alt,
                            mth_episodes_alt,
                            bupxr_episodes_alt,
                            bupo_episodes_alt))[order(link_id,startdt,enddt,-initiate)] |>
  combineepi()


#################
# Save output
#################


write_parquet(indiv_episodes,paste0(outpath,"episodes_indiv.parquet"))
write_parquet(bup_episodes,paste0(outpath,"episodes_bup.parquet"))
write_parquet(any_episodes,paste0(outpath,"episodes_any.parquet"))
write_parquet(any_episodes_alt,paste0(outpath,"episodes_any_alt.parquet"))

toc()


