############################################
############################################
# Create flag for inclusion: aged 18-64 on day 0
# Author: Rachael Ross
#
# Output: incl_age = file with flag for 18-64 on day 0, also includes baseline demographics
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
# Table of unique MOUD initiate dates
list <- list.files(outpath, pattern = "*imoud_dates*", recursive = TRUE) 
imoud_dates <- open_dataset(paste0(outpath, list), format="parquet") |> 
  select(-moud) |>
  mutate(YEAR=year(mouddate)) |>
  distinct() |>
  collect() 
  

# Demographic and enrollment files
filelist <- list.files(inraw, pattern = "*base.*\\.parquet$", recursive = TRUE) 
debase <- open_dataset(paste0(inraw, filelist), format="parquet") |>
  mutate(link_id = ifelse(grepl("^\\s*$", BENE_ID)==TRUE|is.na(BENE_ID),
                          paste0(MSIS_ID,STATE_CD),
                          paste0(BENE_ID,STATE_CD)),
         YEAR=as.numeric(RFRNC_YR)) |>
  select(link_id,YEAR,BIRTH_DT,SEX_CD,RACE_ETHNCTY_CD,MRTL_STUS_CD,PRMRY_LANG_GRP_CD,
         CTZNSHP_IND,HSEHLD_SIZE_CD,VET_IND,INCM_CD,TANF_CASH_CD,SSI_STATE_SPLMT_CD) |>
  filter(link_id %in% imoud_dates$link_id) |>
  distinct() |>
  collect() |>
  mutate(start=update(today(),month=1,year=YEAR,day=1),
         end=update(start,month=12,day=31))

#################
# Processing
#################

# Merge moud dates file with demographics & create desired variables
setDT(imoud_dates)[ , dt2 := mouddate]
setDT(debase)
setkey(debase,link_id,start,end) 


demos <- foverlaps(imoud_dates, debase, 
          by.x = c("link_id", "mouddate", "dt2"),
          by.y = c("link_id", "start", "end"),
          type = "within", mult = "first", nomatch=NA) #if duplicates, just taking first row
demos[, dem_age := as.period(interval(start = BIRTH_DT, end = mouddate))$year
      ][,incl_age := fifelse(is.na(dem_age),0,ifelse(18<=dem_age&dem_age<=64,1,0))
        ][, `:=`(dem_male=ifelse(SEX_CD=="M",1,
                             ifelse(SEX_CD=="F",0,NA)),
                 dem_re=factor(ifelse(RACE_ETHNCTY_CD %in% c(1:8),as.numeric(RACE_ETHNCTY_CD),NA),
                               labels = c("White, non-Hispanic", "Black, non-Hispanic", "Asian, non-Hispanic", "AIAN, non-Hispanic",
                                          "Hawaiin/Pacific Islander", "Hispanic, all races")),
                 dem_re_wnh=ifelse(RACE_ETHNCTY_CD==1,1,
                               ifelse(RACE_ETHNCTY_CD %in% c(2,3,4,5,6,7,8),0,NA)),
                 dem_re_bnh=ifelse(RACE_ETHNCTY_CD==2,1,
                               ifelse(RACE_ETHNCTY_CD %in% c(1,3,4,5,6,7,8),0,NA)),
                 dem_re_anh=ifelse(RACE_ETHNCTY_CD==3,1,
                               ifelse(RACE_ETHNCTY_CD %in% c(1,2,4,5,6,7,8),0,NA)),
                 dem_re_ainh=ifelse(RACE_ETHNCTY_CD==4,1,
                               ifelse(RACE_ETHNCTY_CD %in% c(1,2,3,5,6,7,8),0,NA)),
                 dem_re_pinh=ifelse(RACE_ETHNCTY_CD==5,1,
                                ifelse(RACE_ETHNCTY_CD %in% c(1,2,3,4,6,7,8),0,NA)),
                 dem_re_h=ifelse(RACE_ETHNCTY_CD==7,1,
                                ifelse(RACE_ETHNCTY_CD %in% c(1,2,3,4,6,5,8),0,NA)),
                 dem_married=ifelse(MRTL_STUS_CD %in% c("01","02","03","04","05","06","07","08"),1,
                                ifelse(MRTL_STUS_CD %in% c("09","10","11","12","13","14"),0,NA)),
                 dem_language=factor(ifelse(PRMRY_LANG_GRP_CD=="E",1,
                                 ifelse(PRMRY_LANG_GRP_CD=="S",2,
                                        ifelse(PRMRY_LANG_GRP_CD %in% c("O","R","C"),3,NA))),
                                 labels = c("English","Spanish","Other")),
                 dem_english=ifelse(PRMRY_LANG_GRP_CD=="E",1,
                                ifelse(PRMRY_LANG_GRP_CD %in% c("S","O","R","C"),0,NA)),
                 dem_spanish=ifelse(PRMRY_LANG_GRP_CD=="S",1,
                                ifelse(PRMRY_LANG_GRP_CD %in% c("E","O","R","C"),0,NA)),
                 dem_citizen=ifelse(CTZNSHP_IND==1,1,
                                ifelse(CTZNSHP_IND==0,0,NA)),
                 dem_household=ifelse(HSEHLD_SIZE_CD %in% c("01","02","03","04","05"),as.numeric(HSEHLD_SIZE_CD),NA),
                 dem_hshold_cat=factor(ifelse(HSEHLD_SIZE_CD %in% c("01","02"),as.numeric(HSEHLD_SIZE_CD),
                                   ifelse(HSEHLD_SIZE_CD %in% c("03","04","05"),3,NA))
                                   #,labels = c("1 person", "2 people", ">2 people")
                                   ),
                 dem_poverty=ifelse(INCM_CD %in% c("01","02"),1,
                                ifelse(INCM_CD %in% c("03","04","05","06","07","08"),0,NA)),
                 dem_tanf=ifelse(TANF_CASH_CD==2,1,
                             ifelse(TANF_CASH_CD %in% c(0,1),0,NA)),
                 #ssi=ifelse(SSI_IND==1,1,
                 #           ifelse(SSI_IND==0,0,NA)),
                 dem_ssi=ifelse(SSI_STATE_SPLMT_CD  %in% c("001","002"),1,
                            ifelse(SSI_STATE_SPLMT_CD=="000",0,NA)))]


incl_age <- demos[,.(link_id,mouddate,incl_age)]
demos1 <- demos |> select(link_id,mouddate,starts_with("dem"))

#################
# Save output
#################

write_parquet(incl_age,paste0(outpath,"incl_age.parquet"))
print(paste0(nrow(incl_age),"in incl_age"))
saveRDS(demos1,paste0(outpath,"base_demos.rds"))
toc()