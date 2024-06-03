############################################-
############################################-
# Create analysis dataset
# Author: Rachael Ross
############################################-
############################################-

# See if using full or sample from command line
dattype <- commandArgs(trailingOnly=TRUE)

#################-
# Libraries----
#################-
library(tidyverse)
library(arrow)
library(rlang)
library(data.table)

#################-
# Paths----
#################-

projpath <- "/home/data/moud/01_medicaidCE/"

# Set paths
if (dattype=="full") {
  #AWS/full
  path <- paste0(projpath,"intermediate/")
  clean <- paste0(projpath,"clean/")
} else if (dattype=="sample") {
  #AWS/sample
  path <- paste0(projpath,"sample/intermediate/")
  clean <- paste0(projpath,"sample/clean/")
}

# Output
outpath <- "/home/rr3551/moudr01/output/01_medicaidCE/"

#################-
# Make flat files ----
#################-

### Combine files

## baseline and followup files
base_files <- list.files(path, pattern = "^base*", recursive = T)
fu_files <- list.files(path, pattern = "^fu*", recursive = T)

files <- c(base_files,fu_files)
all_files <- map(files, ~readRDS(paste0(path, .x)))

dat <- reduce(all_files, ~left_join(.x,.y), by=c("link_id","mouddate")) 

## create outcome variables
dat_without <- dat |>
  mutate(fudt_180 = mouddate + days(180),
         fudays_180 = 180,
         # Outcome event 1, definition a
         event1a_days = pmin(fudays_180,fudays_studyend,fudays_discona,fudays_death,fudays_disenroll,na.rm=TRUE),
         event1a_wks = ceiling(event1a_days/7),
         event1a_mos = ceiling(event1a_days/30),
         event1a_death = case_when(event1a_days==fudays_death ~ 1,
                                  .default = 0),
         event1a_discon = case_when(event1a_days==fudays_discona ~ 1,
                                     .default = 0),
         event1a_ltfu = case_when(event1a_days==fudays_disenroll ~ 1,
                                 .default = 0),
         event1a_delta = case_when(event1a_death==1 ~ 1,
                                   event1a_discon==1 ~ 1,
                                  .default = 0),
         event1a_Y180 = case_when(event1a_delta==1 ~ 1,
                                  event1a_days==180 ~ 0,
                                  .default = NA),
         event1a_C180 = case_when(is.na(event1a_Y180) ~ 0,
                                  .default = 1),
         
         # for sensitivity analysis where LTFU is included int he outcome
         eventse1a_days = event1a_days,
         eventse1a_wks = event1a_wks,
         eventse1a_mos = event1a_mos,
         eventse1a_delta =  case_when(event1a_death==1 ~ 1,
                                      event1a_discon==1 ~ 1,
                                      event1a_ltfu==1 ~ 1,
                                      .default = 0),
         eventse1a_Y180 = case_when(eventse1a_delta==1 ~ 1,
                                     eventse1a_days==180 ~ 0,
                                     .default = NA),
         eventse1a_C180 = case_when(is.na(eventse1a_Y180) ~ 0,
                                     .default = 1),
         
         # for 2nd sensitivity with alternative grace period 
         eventseg1a_days = pmin(fudays_180,fudays_studyend,fudays_discona_alt,fudays_death,fudays_disenroll,na.rm=TRUE),
         eventseg1a_wks = ceiling(eventseg1a_days/7),
         eventseg1a_mos = ceiling(eventseg1a_days/30),
         eventseg1a_death = case_when(eventseg1a_days==fudays_death ~ 1,
                                   .default = 0),
         eventseg1a_discon = case_when(eventseg1a_days==fudays_discona_alt ~ 1,
                                    .default = 0),
         eventseg1a_delta = case_when(eventseg1a_death==1 ~ 1,
                                   eventseg1a_discon==1 ~ 1,
                                   .default = 0),
         eventseg1a_Y180 = case_when(eventseg1a_delta==1 ~ 1,
                                  eventseg1a_days==180 ~ 0,
                                  .default = NA),
         eventseg1a_C180 = case_when(is.na(eventseg1a_Y180) ~ 0,
                                  .default = 1),
         
         # Outcome event 1, definition b
         event1b_days = pmin(fudays_180,fudays_studyend,fudays_disconb,fudays_death,fudays_disenroll,na.rm=TRUE),
         event1b_wks = ceiling(event1b_days/7),
         event1b_mos = ceiling(event1b_days/30),
         event1b_death = case_when(event1b_days==fudays_death ~ 1,
                                   .default = 0),
         event1b_discon = case_when(event1b_days==fudays_disconb ~ 1,
                                    .default = 0),
         event1b_ltfu = case_when(event1b_days==fudays_disenroll ~ 1,
                                  .default = 0),
         event1b_delta = case_when(event1b_death==1 ~ 1,
                                   event1b_discon==1 ~ 1,
                                   .default = 0),
         event1b_Y180 = case_when(event1b_delta==1 ~ 1,
                                  event1b_days==180 ~ 0,
                                  .default = NA),
         event1b_C180 = case_when(is.na(event1b_Y180) ~ 0,
                                  .default = 1),
         
         # Outcome event 1, definition c
         event1c_days = pmin(fudays_180,fudays_studyend,fudays_disconc,fudays_death,fudays_disenroll,na.rm=TRUE),
         event1c_wks = ceiling(event1c_days/7),
         event1c_mos = ceiling(event1c_days/30),
         event1c_death = case_when(event1c_days==fudays_death ~ 1,
                                   .default = 0),
         event1c_discon = case_when(event1c_days==fudays_disconc ~ 1,
                                    .default = 0),
         event1c_ltfu = case_when(event1c_days==fudays_disenroll ~ 1,
                                  .default = 0),
         event1c_delta = case_when(event1c_death==1 ~ 1,
                                   event1c_discon==1 ~ 1,
                                   .default = 0),
         event1c_Y180 = case_when(event1c_delta==1 ~ 1,
                                  event1c_days==180 ~ 0,
                                  .default = NA),
         event1c_C180 = case_when(is.na(event1c_Y180) ~ 0,
                                  .default = 1),

         # Outcome event 2
         event2_days = pmin(fudays_180,fudays_studyend,fudays_overdose,fudays_death,fudays_disenroll,na.rm=TRUE),
         event2_wks = ceiling(event2_days/7),
         event2_mos = ceiling(event2_days/30),
         event2_death = case_when(event2_days==fudays_death ~ 1,
                                  .default = 0),
         event2_overdose = case_when(event2_days==fudays_overdose ~ 1,
                                  .default = 0),
         event2_ltfu = case_when(event2_days==fudays_disenroll ~ 1,
                                 .default = 0),
         event2_delta = case_when(event2_death==1 ~ 1,
                                  event2_overdose==1 ~ 1,
                                  .default = 0),
         event2_Y180 = case_when(event2_delta==1 ~ 1,
                                 event2_days==180 ~ 0,
                                 .default = NA),
         event2_C180 = case_when(is.na(event2_Y180) ~ 0,
                                  .default = 1),
         
         # Outcome event 2 alternative for sensitivity analysis that include unspecified codes
         eventse2_days = pmin(fudays_180,fudays_studyend,fudays_overdosealt,fudays_death,fudays_disenroll,na.rm=TRUE),
         eventse2_wks = ceiling(eventse2_days/7),
         eventse2_mos = ceiling(eventse2_days/30),
         eventse2_death = case_when(eventse2_days==fudays_death ~ 1,
                                  .default = 0),
         eventse2_overdose = case_when(eventse2_days==fudays_overdosealt ~ 1,
                                     .default = 0),
         eventse2_delta = case_when(eventse2_death==1 ~ 1,
                                  eventse2_overdose==1 ~ 1,
                                  .default = 0),
         eventse2_Y180 = case_when(eventse2_delta==1 ~ 1,
                                 eventse2_days==180 ~ 0,
                                 .default = NA),
         eventse2_C180 = case_when(is.na(eventse2_Y180) ~ 0,
                                 .default = 1))

## merge with each cohort
eachcohort <- function(num){
  cohort <- readRDS(paste0(path,"cohort",num,".rds"))
  #nrow(dat)==nrow(cohort) #Check that true
  
  withdat <- cohort |>
    left_join(dat_without, by=c("link_id","mouddate","moud"))
  
  ### Save analysis dataset
  saveRDS(withdat, paste0(clean,"analysisdat_flat",num,".rds"))
}

numbers <- 7#seq(1:6)
walk(numbers,eachcohort)

#################-
# Make long and wide files ----
#################-

# Function to make long variables for specific outcome
makeoutlong <- function(out,dat,interval){
  #time <- ensym(timevar)
  #delta <- ensym(deltavar)
  time <- sym(paste0("event",out,"_",interval))
  delta <- sym(paste0("event",out,"_delta"))
  
  ## Make long
  long <- dat %>% 
    select(link_id,mouddate,moud,!!time,!!delta) |>
    uncount(max(!!time)+1, .remove = FALSE) |>
    mutate(period = row_number()-1,
           C = case_when(period==0 ~ 1,
                         period < !!time ~ 1,
                         period > !!time ~ NA,
                         !!delta == 0 ~ 0,
                         .default = NA),
           Y = case_when(period==0 ~ 0,
                         period < !!time ~ 0,
                         period == !! time & !!delta ==0 ~ 0,
                         period > !! time & !!delta == 0 ~ NA,
                         .default = 1), .by=c(link_id,mouddate,moud),
           outcome=out) |>
    select(-starts_with("event")) |>
    as.data.table()
  return(long)
}

# Function to make wide data from long file
makeoutwide <- function(long){
  wide <- dcast(melt(long, id.vars=c("link_id","mouddate","moud","period","outcome")), 
                link_id+mouddate+moud~outcome+variable+period) |>
    select(link_id,mouddate,moud,everything())
  
  names(wide)[4:length(names(wide))] <- paste0("out",names(wide)[4:length(names(wide))])
  return(wide)
}


# Function to make all the long and wide datasets (calls above functions for each outcome)
makedatwide <- function(num,interval){
  
  ## Load cohort data
  flatdat <- readRDS(paste0(clean,"analysisdat_flat",num,".rds"))
  
  ## Make long/wide for each outcome
  outcomes <- c("1a","1b","1c","2", "se1a", "se2", "seg1a")
  longouts_list <-  map(outcomes,makeoutlong,dat=flatdat,interval=interval)
  longouts <- reduce(longouts_list, ~rbind(.x,.y))
  
  wideouts <-  reduce(map(longouts_list,makeoutwide), 
                      ~left_join(.x,.y), by=c("link_id","mouddate","moud"))

  ## Merge in baseline vars
  allwide <- flatdat |>
    left_join(wideouts, by=c("link_id","mouddate","moud"))
  alllong <- flatdat |>
    left_join(longouts, by=c("link_id","mouddate","moud")) |>
    mutate(uniqueid = rleid(link_id,mouddate)) # create a unique id - should have same number as link_id b/c restricted to one period per person
  
  ## Save wide analysis dataset
  saveRDS(alllong, paste0(clean,"analysisdat_long",num,".rds"))
  saveRDS(allwide, paste0(clean,"analysisdat_wide",num,".rds"))
}

#numbers <- 7#seq(1:6)
#walk(numbers,makedatwide,interval="wks")

# flatdat <- readRDS(paste0(clean,"analysisdat_flat",2,".rds")) |>
#   mutate(deathafter = fudays_death - fudays_disenroll,
#          overdoseafter = fudays_overdose - fudays_disenroll,
#          look = ifelse(deathafter %in% seq(1:20),1,
#                        ifelse(overdoseafter %in% seq(1:20),1,0))) |>
#   dplyr::select(link_id,mouddate,deathafter,overdoseafter, starts_with("fudays"),look) |>
#   filter(look==1)
# 
# 
# names(flatdat)
  
             