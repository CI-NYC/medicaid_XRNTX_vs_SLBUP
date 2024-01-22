############################################
############################################
# Create flag for exclusion: bup/meth course <= 7days long
# Author: Rachael Ross
#
# Output: excl_tooshort = file with flag marking courses <= 7 days
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

#################
# Load files
#################
tic()
# list <- list.files(outpath, pattern = "*moud_ndc_*", recursive = TRUE)
# moud_ndc <- open_dataset(paste0(outpath, list), format="parquet") 
# 
# list <- list.files(outpath, pattern = "*moud_px_*", recursive = TRUE)
# moud_px <- open_dataset(paste0(outpath, list), format="parquet") 

list <- list.files(outpath, pattern = "*anymoud_*", recursive = TRUE)
anymoud <- open_dataset(paste0(outpath, list), format="parquet") 

list <- list.files(outpath, pattern = "*imoud_*", recursive = TRUE)
imoud <- open_dataset(paste0(outpath, list), format="parquet") |>
  select(link_id,moud,mouddate) |>
  distinct() |>
  collect() |>
  mutate(endwind = mouddate + days(earlywindow))


#################
# Processing 
#################

### Methadone
mthindex <- imoud |> filter(moud=="mth") |> select(link_id,mouddate,moud,endwind) |> as.data.table()
methpxs <- anymoud |> 
  filter(moud == "mth" & initiate==1) |> 
  select(link_id,mouddate,days,source) |> 
  rename(dt=mouddate) |>
  mutate(dt2 = dt) |>
  collect() |>
  as.data.table()

setkey(methpxs,link_id,dt,dt2)

mthoverlap <-  foverlaps(mthindex, methpxs, 
                         by.x = c("link_id", "mouddate", "endwind"),
                         by.y = c("link_id", "dt", "dt2"),
                         type = "any", mult = "all", nomatch=NA)[,dayssince := dt - mouddate
                         ][dayssince==0 | source=="os"] #Inpatient only allowed on day 0

### Bup
bupindex <- imoud |> filter(moud=="bup") |> select(link_id,mouddate,moud,endwind) |> as.data.table()
bupcodes <- anymoud |> 
  filter(moud =="bup" & initiate==1) |> 
  select(link_id,mouddate,days,source) |> 
  rename(dt=mouddate) |>
  mutate(dt2 = dt) |>
  collect() |>
  as.data.table()

setkey(bupcodes,link_id,dt,dt2)

bupoverlap <-  foverlaps(bupindex, bupcodes, 
                         by.x = c("link_id", "mouddate", "endwind"),
                         by.y = c("link_id", "dt", "dt2"),
                         type = "any", mult = "all", nomatch=NA)[,dayssince := dt - mouddate
                         ][dayssince==0 | source=="os"] #Inpatient only allowed on day 0


### Create flags
crtflags <- function(med){
  
  index <- imoud |> filter(moud==med) |> select(link_id,mouddate,moud,endwind) |> as.data.table()
  pxndc <- anymoud |> 
    filter(moud ==med & initiate==1) |> 
    select(link_id,mouddate,days,source) |> 
    rename(dt=mouddate) |>
    mutate(dt2 = dt) |>
    collect() |>
    as.data.table()
  
  setkey(pxndc,link_id,dt,dt2)
  
  overlap <-  foverlaps(index, pxndc, 
                           by.x = c("link_id", "mouddate", "endwind"),
                           by.y = c("link_id", "dt", "dt2"),
                           type = "any", mult = "all", nomatch=NA)[,dayssince := dt - mouddate
                           ][dayssince==0 | source=="os"] #Inpatient only allowed on day 0
  
  overlap[, .(days = sum(days,na.rm = TRUE),
          last = max(dayssince)),by=.(link_id,mouddate,moud)
  ][, `:=` (aftershort = case_when(last >= shortcourse ~ 1,
                                   .default = 0))
  ][,excl_tooshort := case_when(days <= shortcourse & aftershort == 0 ~ 1,
                                .default = 0)]
}


mth_flags <- crtflags("mth")
bup_flags <- crtflags("bup")
flags <- rbind(mth_flags,bup_flags) |> select("link_id","mouddate","moud","excl_tooshort")

### Merge back in
excl_tooshort <- imoud |>
  left_join(flags, by=c("link_id","mouddate","moud")) |>
  mutate(excl_tooshort = case_when(moud=="ntx" ~ 0,
                                   .default = excl_tooshort)) |> as.data.table() |>
  select(-endwind)

#################
# Save output
#################

write_parquet(excl_tooshort,paste0(outpath,"excl_tooshort.parquet"))
print(paste0(nrow(excl_tooshort),"in excl_tooshort"))
toc()





