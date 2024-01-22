############################################
############################################
# Deal with ambiguous MOUD codes (H0033 & CPT96372) & remove ndcs claims in ip/os that are dups of pxs
# Author: Rachael Ross
#
# Output: anymoud = unique list of link_id, date, and moud (updated to clarify ambiguous codes - when possible)
#         imoud_date = unique link_id, date, moud for those that can be initiating fills
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

#################
# Load files
#################
tic()
list <- list.files(outpath, pattern = "*moud_ndc_raw*", recursive = TRUE)
moud_ndc <- open_dataset(paste0(outpath, list), format="parquet") 

list <- list.files(outpath, pattern = "*moud_px_raw*", recursive = TRUE)
moud_px <- open_dataset(paste0(outpath, list), format="parquet") 

list <- list.files(inproc, pattern = "*ed_.*\\.parquet$", recursive = TRUE) 
ed <- open_dataset(paste0(inproc, list), format="parquet") |>
  select(link_id, CLM_ID) |>
  distinct() |>
  collect() |> 
  mutate(ed=1) |>
  as.data.table()

#################
# Processing
#################

### Combine procedures and ndcs

# procedures
moud_px_ <- moud_px |>
  select(link_id,CLM_ID,moud,initiate,PRCDR_CD_DT,route,days,PRCDR_CD,source) |>
  rename(mouddate=PRCDR_CD_DT,
         code=PRCDR_CD) |>
  mutate(px=1) |>
  collect()

# ndcs
moud_ndc_ <- moud_ndc |>
  select(link_id,CLM_ID,moud,initiate,RX_FILL_DT,route,DAYS_SUPPLY,NDC,source) |>
  rename(mouddate=RX_FILL_DT,
         code=NDC) |>
  mutate(px=0) |>
  collect() |>
  mutate(days=case_when(!is.na(DAYS_SUPPLY)~DAYS_SUPPLY,
                        moud=="ntx"&route=="injection"~ntxinjlength,
                        moud=="bup"&route=="oral"~bupndciposlength,
                        .default=NA)) |>
  select(-DAYS_SUPPLY)
  

# test <- moud_ndc_ |> filter(is.na(days))
# table(test$moud,test$route)

# combined
moud_combo <- rbind(moud_px_,moud_ndc_) |> as.data.table()

### Deal with ambiguous codes - assign moud based on closest non-ambiguous code in time (within window defined by ambcode_days)
findclosest <- function(data,tag1,tag2,window){
  
  amb_code <- copy(data)[moud==tag1
                         ][, `:=` (start=mouddate-window,end=mouddate+window)]
  possible <- copy(data)[moud %in% tag2][, `:=` (dt=mouddate,
                                                 dt2=mouddate)][,.(link_id,dt,dt2,moud)]
  setkey(possible,link_id,dt,dt2)
  foverlaps(amb_code, possible, 
                    by.x = c("link_id", "start", "end"),
                    by.y = c("link_id", "dt", "dt2"),
                    type = "any", mult = "all", nomatch=NULL)[,diff := abs(dt-mouddate)
                    ][order(link_id,mouddate,CLM_ID,code,diff)
                    ][,head(.SD, 1), by = .(link_id,mouddate,CLM_ID,code)
                    ][,.(link_id,mouddate,CLM_ID,moud,source,code,days,px)]
}

H0033 <- findclosest(moud_combo,"bup_meth",c("bup","mth"),ambcode_days) |> mutate(initiate=1,route="oral")
CPT96372 <- findclosest(moud_combo,"bup_ntx",c("bup","ntx"),ambcode_days) |> mutate(initiate=ifelse(moud=="ntx",1,0),
                                                                                    route="injection")
# Update moud_combo file
updated <-   rbind(moud_combo |> 
                     filter(!(moud %in% c("bup_meth","bup_ntx"))),
                   moud_combo |> 
                     filter(moud=="bup_meth") |> 
                     anti_join(H0033, by=c("link_id","mouddate")),
                   H0033,
                   moud_combo |> 
                     filter(moud=="bup_ntx") |> 
                     anti_join(CPT96372, by=c("link_id","mouddate")),
                   CPT96372) |> as.data.table()


# Identify ip/os ndcs that are copies of ip/os px codes (matched either on date or CLM_ID)
dups <- unique(rbind(merge(updated |> filter(source != "rx" & px==0),
                    updated |> filter(px==1) |> select(link_id,moud,mouddate,route) |> distinct(),
                    by.x=c("link_id","mouddate","moud","route"), # matched on mouddate
                    by.y=c("link_id","mouddate","moud","route"))[,.(link_id,moud,CLM_ID,mouddate,route)],
              merge(updated |> filter(source != "rx" & px==0),
                    updated |> filter(px==1) |> select(link_id,moud,CLM_ID,route) |> distinct(),
                    by.x=c("link_id","CLM_ID","moud","route"), #matched on clm id
                    by.y=c("link_id","CLM_ID","moud","route"))[,.(link_id,moud,CLM_ID,mouddate,route)]))



# Remove dup NDCs from combined file and add ED flag
updated_nodup <- rbind(updated |>
                         filter(px==0) |>
                         anti_join(dups),
                       updated |>
                         filter(px==1)) |>
  left_join(ed, by=c("link_id","CLM_ID")) 


# Initiate fills only
imoud <- updated_nodup |>
  filter(initiate==1) |>
  select(link_id,mouddate,moud) |>
  distinct() |>
  mutate(w_end = floor_date(mouddate, unit="month"),
         wash_start = w_end - days(washoutday),
         washalt_start = w_end - days(washoutday_alt),
         enroll_start = w_end - months(enrollmo),
         enrollalt_start = w_end - months(enrollmo_alt))

#################
# Save output
#################


write_parquet(updated_nodup,paste0(outpath,"anymoud.parquet"))
write_parquet(imoud,paste0(outpath,"imoud_dates.parquet"))

print(paste0(nrow(updated_nodup),"in anymoud_update"))
print(paste0(nrow(imoud),"in imoud"))
toc()


