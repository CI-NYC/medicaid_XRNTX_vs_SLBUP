############################################
############################################
# Create flag for exclusion: NO continuous enrollment in washout window
# Author: Rachael Ross
#
# Output: excl_nocontenr = file with flag for NO continuous enrollment in washout window
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
# 
# #################
# # Defined windows
# #################
# 
# assumptions <- read.csv(paste0(codes,"assumptions.csv"))
# 
# washoutmo <- assumptions[1,3] #months in washout
# mindayscover <- assumptions[2,3] #minimum days with coverage in a month to be considered enrolled

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

# Enrollment data
list <- list.files(inproc, pattern = "*enroll_*", recursive = TRUE) 
enroll_df <- open_dataset(paste0(inproc, list), format="parquet") |> 
  filter(link_id %in% imoud_dates$link_id) 


#################
# Processing
#################

# Add a date to enroll_monthly, also add flags for enrolled and eligible (enrolled, full benefits, nodual)
enroll_withdt <- enroll_df |>
  collect() |>
  mutate(enrolldt=update(today(),month=MONTH,year=YEAR,day=1),
         dt2=enrolldt,
         enrolled = ifelse(is.na(MDCD_ENRLMT_DAYS),0,
                           ifelse(MDCD_ENRLMT_DAYS<mindayscover,0,1)),
         eligible = ifelse(enrolled==0,0,
                           ifelse(nodual_flag %in% c(0,NA),0,
                                  ifelse(fullbnfts_flag %in% c(0,NA),0,1))))

# Merge them, grab all months in imoud window in enrollment file

# # SQL was notably slower here
# washout <- sqldf("SELECT a.link_id, a.mouddate, a.w_start, 
#               b.enrolldt, b.MONTH, b.YEAR, b.eligible
#               FROM (SELECT distinct link_id, mouddate, w_start from imoud_dates) as a left join enroll_withdt as b on 
#               a.link_id=b.link_id and a.w_start <= b.enrolldt and b.enrolldt <= a.mouddate)
#
# washout <- imoud_dates |>
#   left_join(enroll_withdt, by=c("link_id"), relationship="many-to-many") |>
#   filter(w_start <= enrolldt & enrolldt <= mouddate) 

setDT(imoud_dates)  
setDT(enroll_withdt)
setkey(enroll_withdt,link_id,enrolldt,dt2) 

excl_nocontenr <-  foverlaps(imoud_dates, enroll_withdt, 
                     by.x = c("link_id", "enroll_start", "mouddate"),
                     by.y = c("link_id", "enrolldt", "dt2"),
                     type = "any", mult = "all", nomatch=NA)[, .(eligsum = sum(eligible)), by=.(link_id,mouddate)
                                                             ][, `:=` (excl_nocontenr = ifelse(eligsum>enrollmo,0,1),
                                                                       eligsum=NULL)]
excl_nocontenralt <-  foverlaps(imoud_dates, enroll_withdt, 
                             by.x = c("link_id", "enrollalt_start", "mouddate"),
                             by.y = c("link_id", "enrolldt", "dt2"),
                             type = "any", mult = "all", nomatch=NA)[, .(eligsum = sum(eligible)), by=.(link_id,mouddate)
                             ][, `:=` (excl_nocontenr_alt = ifelse(eligsum>enrollmo_alt,0,1),
                                       eligsum=NULL)]

excl <- merge(excl_nocontenr,excl_nocontenralt)


#################
# Save output
#################

write_parquet(excl,paste0(outpath,"excl_nocontenr.parquet"))
print(paste0(nrow(excl),"in excl_nocontenr"))
toc()