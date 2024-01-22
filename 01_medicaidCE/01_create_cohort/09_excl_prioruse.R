############################################
############################################
# Create 2 flags for exclusion: mulitple MOUDs on day 0 and trt episode overlapping with washout
# Author: Rachael Ross
#
# Output: excl_prioruse = file with 2 flags 1) for multiple moud on day 0 & 
#                                           2) for prior use in washout
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
# ntx_priormbuse <- assumptions[3,3] # days prior to ntx where mth/bup allowed (for detox)

#################
# Load files
#################
tic()
list <- list.files(outpath, pattern = "*anymoud_*", recursive = TRUE)
anymoud <- open_dataset(paste0(outpath, list), format="parquet") 

list <- list.files(outpath, pattern = "*imoud_*", recursive = TRUE)
imoud <- open_dataset(paste0(outpath, list), format="parquet") 

episodes_indiv <- open_dataset(paste0(outpath, "episodes_indiv.parquet"), format="parquet") |>
  collect()
episodes_any <- open_dataset(paste0(outpath, "episodes_any.parquet"), format="parquet") |>
  collect()
episodes_bup <- open_dataset(paste0(outpath, "episodes_bup.parquet"), format="parquet") |>
  collect()


#################
# Check for multiples on day 0
#################

# Check for other drugs on the same day
multmoud <- function(tag,othtags){
  imoud |>
    filter(moud==tag) |>
    inner_join(anymoud |> filter(moud %in% othtags), by=c("link_id","mouddate")) |>
    select(link_id,mouddate,moud.x) |>
    rename(moud=moud.x) |>
    distinct() |>
    collect()
}

m_multmoud <- multmoud("mth",c("ntx","bup")) #do not need to include bup_meth because of prior cleaning
b_multmoud <- multmoud("bup",c("ntx","mth","bup_ntx")) #do not need to include bup_meth because of prior cleaning
n_multmoud <- multmoud("ntx",c("bup","mth","bup_meth"))


# Check for a non-initiate forms on the same day
othform <- function(tag){
  imoud |>
    filter(moud==tag) |>
    inner_join(anymoud |> filter(moud==tag&initiate==0), by=c("link_id","mouddate")) |>
    select(link_id,mouddate,moud.x) |>
    rename(moud=moud.x) |>
    collect()
}

m_oth <- othform("mth")
b_oth <- othform("bup")
n_oth <- othform("ntx")


# Combine and create flag
rmv <- rbind(m_multmoud,b_multmoud,n_multmoud,
             m_oth,b_oth,n_oth) |>
  mutate(excl_multiday0=1) |>
  distinct()

excl_multiday0 <- imoud |>
  left_join(rmv, by=c("link_id","mouddate","moud")) |>
  mutate(excl_multiday0 = ifelse(is.na(excl_multiday0),0,1)) |>
  select(link_id,mouddate,moud,excl_multiday0) |>
  collect()


#################
# Assess prior use
#################

# Assess prior use of any moud in window - for bup and mth
mthbuprior <- imoud |> 
  collect() |>
  filter(moud %in% c("bup","mth")) |>
  select(-w_end,-starts_with("enroll")) |>
  inner_join(episodes_any, 
               by=c("link_id"),
               relationship = "many-to-many") |>
  filter((wash_start <= enddt & enddt < mouddate)| # ends during washout
             (wash_start <= startdt & startdt < mouddate)| #starts during washout
             (startdt < wash_start & mouddate <= enddt)) |> # starts before washout and ends on/after mouddate
  mutate(excl_prioruse = 1,
         # endsduring = ifelse(wash_start <= enddt & enddt < mouddate,1,0),
         # startsduring = ifelse(wash_start <= startdt & startdt < mouddate,1,0),
         # overlaps = ifelse(startdt < wash_start & mouddate <= enddt,1,0),
         # startvsmoud = mouddate - startdt,
         # endvsmoud = mouddate - enddt,
         # startvswash = wash_start - startdt,
         # endvswash = wash_start - enddt
         ) |> 
  # data.table()
  # mthbuprior[,.N,by=.(endsduring,startsduring,overlaps)]
  # mthbuprior[endsduring==1,.(min_startvswash = min(startvswash),
  #                        max_startvswash = max(startvswash),
  #                        min_endvswash = min(endvswash),
  #                        max_endvswash = max(endvswash),
  #                        min_startvsmoud = min(startvsmoud),
  #                        max_startvsmoud = max(startvsmoud),
  #                        min_endvsmoud = min(endvsmoud),
  #                        max_endvsmoud = max(endvsmoud))]
  mutate(excl_prioruse_alt = case_when(washalt_start <= enddt & enddt < mouddate ~ 1, # ends during washout
                                      washalt_start <= startdt & startdt < mouddate ~ 1, #starts during washout
                                      startdt < washalt_start & mouddate <= enddt ~ 1, # overlaps
                                     .default = 0)) |>
  select(link_id,mouddate,moud,excl_prioruse,excl_prioruse_alt) |>
  data.table()

mthbuprior <- mthbuprior[,.(excl_prioruse=max(excl_prioruse),
                            excl_prioruse_alt=max(excl_prioruse_alt)), 
                         by=.(link_id,mouddate,moud)]
# mthbuprior[,.N,by=.(excl_prioruse,excl_prioruse_alt)]

# naltrexone - look at prior ntx use
ntxprior1 <- imoud |> 
  collect() |>
  filter(moud==c("ntx")) |>
  inner_join(episodes_indiv |> filter(trt=="ntx"), 
             by=c("link_id"),
             relationship = "many-to-many") |>
  filter((wash_start <= enddt & enddt < mouddate)| # ends during washout
           (wash_start <= startdt & startdt < mouddate)| #starts during washout
           (startdt < wash_start & mouddate <= enddt)) |>
  mutate(excl_prioruse = 1,
         excl_prioruse_alt = case_when(washalt_start <= enddt & enddt < mouddate ~ 1, # ends during washout
                                      washalt_start <= startdt & startdt < mouddate ~ 1, #starts during washout
                                      startdt < washalt_start & mouddate <= enddt ~ 1, # overlaps
                                      .default = 0)) |>
  select(link_id,mouddate,moud,excl_prioruse,excl_prioruse_alt) |>
  data.table()

# ntxprior1[,.N,by=.(excl_prioruse,excl_prioruse_alt)]

# natlrexone - prior use of mth or bup
ntxprior2 <- imoud |> 
  collect() |>
  filter(moud == "ntx") |>
  inner_join(episodes_indiv |> filter(trt %in% c("mth","bup")), 
             by=c("link_id"),
             relationship = "many-to-many") |>
  filter((wash_start <= enddt & enddt < mouddate)| # ends during washout
           (wash_start <= startdt & startdt < mouddate)| #starts during washout
           (startdt < wash_start & mouddate <= enddt)) |>
  mutate(noexclude = ifelse(startdt >= (mouddate - ntx_priormbuse) & enddt < mouddate,1,0),
         excl_prioruse = ifelse(noexclude==1,0,1),
         excl_prioruse_alt = case_when(noexclude==1 ~ 0,
                                      washalt_start <= enddt & enddt < mouddate ~ 1, # ends during washout
                                      washalt_start <= startdt & startdt < mouddate ~ 1, #starts during washout
                                      startdt < washalt_start & mouddate <= enddt ~ 1, # overlaps
                                      .default = 0)) |>
  select(link_id,mouddate,moud,excl_prioruse,excl_prioruse_alt) |>
  data.table()

# ntxprior2[,.N,by=.(excl_prioruse,excl_prioruse_alt)]

ntxprior <- rbind(ntxprior1,ntxprior2)[,.(excl_prioruse=max(excl_prioruse),
                                          excl_prioruse_alt=max(excl_prioruse_alt)), 
                                       by=.(link_id,mouddate,moud)]

# ntxprior[,.N,by=.(excl_prioruse,excl_prioruse_alt)]

### Merge with other data
excl_prioruse <- excl_multiday0 |>
  left_join(rbind(mthbuprior,ntxprior), by=c("link_id","mouddate","moud")) |>
  mutate(excl_prioruse = ifelse(is.na(excl_prioruse),0,excl_prioruse),
         excl_prioruse_alt = ifelse(is.na(excl_prioruse_alt),0,excl_prioruse_alt)) 

#excl_prioruse[,.N,by=.(excl_prioruse,excl_prioruse_alt)]
#################
# Save output
#################

write_parquet(excl_prioruse,paste0(outpath,"excl_prioruse.parquet"))
print(paste0(nrow(excl_prioruse),"in excl_prioruse"))
toc()

