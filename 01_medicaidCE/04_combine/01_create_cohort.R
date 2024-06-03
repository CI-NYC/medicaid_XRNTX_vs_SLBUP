############################################
############################################
# Combine incl/exclusion files/get numbers for consort
# Author: Rachael Ross
############################################
############################################

# See if using full or sample from command line
dattype <- commandArgs(trailingOnly=TRUE)

#################
# Libraries
#################
library(tidyverse)
library(arrow)
library(rlang)
library(data.table)

#################
# Paths
#################

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

#################
# Processing
#################

### Combine files

# Fills files
imoud_dates <- open_dataset(paste0(path,list.files(path, pattern = "imoud_dates*", recursive = T))) |>
  collect() |> 
  mutate(STATE=substr(link_id,nchar(link_id)-1,nchar(link_id)),
         YEAR=year(mouddate)) 

#Inclusion and exclusion files
incl_files <- list.files(path, pattern = "incl*", recursive = T)
excl_files <- list.files(path, pattern = "excl*", recursive = T)

files <- c(incl_files,excl_files)
inclexcl_files <- map(files, ~open_dataset(paste0(path, .x))) # read in all cohort inclusions/exclusions
#fileorder <- files %in% c("excl_prioruse.parquet","excl_tooshort.parquet")

#!fileorder
# Reduce all data frames
ie_df0 <- reduce(inclexcl_files[c(1:2)], ~left_join(.x,.y), by=c("link_id","mouddate")) |> collect()
ie_df1 <- reduce(inclexcl_files[c(3:4)], ~left_join(.x,.y), by=c("link_id","mouddate")) |> collect()
ie_df2 <- reduce(inclexcl_files[c(5:6)], ~left_join(.x,.y), by=c("link_id","mouddate")) |> collect()
ie_df3 <- reduce(inclexcl_files[c(7,9)], ~left_join(.x,.y), by=c("link_id","mouddate")) |> collect()
iemoud_df <- reduce(inclexcl_files[c(8,10)], ~left_join(.x,.y), by=c("link_id","mouddate","moud")) |> collect()

inclexcl_df <- imoud_dates |>
  left_join(iemoud_df, by=c("link_id","mouddate","moud")) |>
  left_join(ie_df0, by=c("link_id","mouddate")) |>
  left_join(ie_df1, by=c("link_id","mouddate")) |>
  left_join(ie_df2, by=c("link_id","mouddate")) |>
  left_join(ie_df3, by=c("link_id","mouddate")) |>
  filter(moud != "mth")

#nrow(imoud_dates)==nrow(inclexcl_df) #Check that true - won't be true because excluding methadone here

### Get numbers for flowchart

excl1 <- c("excl_nocontenr",
           "excl_prioruse","excl_multiday0", 
           "excl_tooshort", "excl_studyend", "excl_earlydisenroll", "excl_earlydeath","excl_earlyoverdose", 
           "excl_noouddx",
           "excl_preg","excl_cancer","excl_pall")#,"excl_psychdx")
excl2 <- c("excl_nocontenr",
           "excl_prioruse_alt","excl_multiday0", 
           "excl_tooshort", "excl_studyend", "excl_earlydisenroll", "excl_earlydeath","excl_earlyoverdose", 
           "excl_noouddx",
           "excl_preg","excl_cancer","excl_pall")#,"excl_psychdx")
excl3 <- c("excl_nocontenr_alt",
           "excl_prioruse_alt","excl_multiday0", 
           "excl_tooshort", "excl_studyend", "excl_earlydisenroll", "excl_earlydeath","excl_earlyoverdose", 
           "excl_noouddx",
           "excl_preg","excl_cancer","excl_pall")#,"excl_psychdx")
excl4 <- excl1[!excl1=="excl_noouddx"]
excl5 <- excl2[!excl2=="excl_noouddx"] 
excl6 <- excl3[!excl3=="excl_noouddx"] 

included <- inclexcl_df |> filter(incl_age==1)
setDT(included)


# # For revision response
# box3 <- included |>
#   filter(excl_nocontenr==0) |>
#   filter(excl_prioruse_alt==0 & excl_multiday0==0)
# 
# excluded_prioruse <- included |>
#   filter(excl_nocontenr==0) |>
#   filter(excl_prioruse_alt==1 | excl_multiday0==1)
# 
# among_stayed <- excluded_prioruse[link_id %in% box3$link_id]
# among_excluded <- excluded_prioruse[!(link_id %in% box3$link_id)]
# 
# box3[,.(n=.N,ind=n_distinct(link_id))] 
# excluded_prioruse[,.(n=.N,ind=n_distinct(link_id))] 
# 
# among_excluded[,.(n=.N,ind=n_distinct(link_id))] 
# among_stayed[,.(n=.N,ind=n_distinct(link_id))] 

# getincln <- function(excl){
#   start <- included[,.(n=.N,ind=n_distinct(link_id)),by=.(moud,YEAR,STATE)]
#   start$step_name <- "start"
#   start$step <- 0
#   for (i in 1:length(excl)){
#     numbers <- copy(included)[,sumexcl := rowSums(.SD[,excl[1:i], with=FALSE])
#     ][sumexcl==0
#     ][,.(n=.N,ind=n_distinct(link_id)),by=.(moud,YEAR,STATE)]
#     numbers$step_name <- excl[i]
#     numbers$step <- i
#     start <- rbind(start,numbers)
#   }
#   start
# }
# 
# consort_n <- getincln(excl1)[order(STATE,moud,YEAR,step)]|>
#   mutate(excl_n = ifelse(step==0,0,lag(n) - n),
#          excl_ind = ifelse(step==0,0,lag(ind)- ind)) 

# Stratified by moud and state
getincln_strat <- function(excl){
  cohort <- as.numeric(gsub("\\D", "", deparse(substitute(excl))))
  start <- included[,.(n=.N,ind=n_distinct(link_id)),by=.(moud,STATE)]
  start$step_name <- "start"
  start$step <- 0
  
  for (i in 1:length(excl)){
    numbers <- copy(included)[,sumexcl := rowSums(.SD[,excl[1:i], with=FALSE])
    ][sumexcl==0
    ][,.(n=.N,ind=n_distinct(link_id)),by=.(moud,STATE)]
    numbers$step_name <- excl[i]
    numbers$step <- i
    start <- rbind(start,numbers)
  }

  #Restrict to first episode
  withdups <- copy(included)[,sumexcl := rowSums(.SD[,excl[1:length(excl)], with=FALSE])
  ][sumexcl==0][, firstdt := min(mouddate), by=link_id]
 
  nodups <- withdups[mouddate==firstdt][,.(n=.N,ind=n_distinct(link_id)),by=.(moud,STATE)]
  nodups$step_name <- "excl_multiple"
  nodups$step <- length(excl)+1
  start <- rbind(start,nodups)

  start |> mutate(cohort=cohort)
}

consort_n_strat <- rbind(getincln_strat(excl1)[order(STATE,moud,step)],
                   getincln_strat(excl2)[order(STATE,moud,step)],
                   getincln_strat(excl3)[order(STATE,moud,step)],
                   getincln_strat(excl4)[order(STATE,moud,step)],
                   getincln_strat(excl5)[order(STATE,moud,step)],
                   getincln_strat(excl6)[order(STATE,moud,step)]) |>
  mutate(excl_n = ifelse(step==0,0,lag(n) - n),
         excl_ind = ifelse(step==0,0,lag(ind)- ind)) 

write.csv(consort_n_strat, paste(outpath,"consortn_strat.csv"), row.names=FALSE)


#Stratified only by state
getincln <- function(excl){
  cohort <- as.numeric(gsub("\\D", "", deparse(substitute(excl))))
  start <- included[,.(n=.N,ind=n_distinct(link_id)),by=.(STATE)]
  start$step_name <- "start"
  start$step <- 0
  
  for (i in 1:length(excl)){
    numbers <- copy(included)[,sumexcl := rowSums(.SD[,excl[1:i], with=FALSE])
    ][sumexcl==0
    ][,.(n=.N,ind=n_distinct(link_id)),by=.(STATE)]
    numbers$step_name <- excl[i]
    numbers$step <- i
    start <- rbind(start,numbers)
  }
  
  #Restrict to first episode
  withdups <- copy(included)[,sumexcl := rowSums(.SD[,excl[1:length(excl)], with=FALSE])
  ][sumexcl==0][, firstdt := min(mouddate), by=link_id]
  
  nodups <- withdups[mouddate==firstdt][,.(n=.N,ind=n_distinct(link_id)),by=.(STATE)]
  nodups$step_name <- "excl_multiple"
  nodups$step <- length(excl)+1
  start <- rbind(start,nodups)
  
  start |> mutate(cohort=cohort)
}

consort_n <- rbind(getincln(excl1)[order(STATE,step)],
                         getincln(excl2)[order(STATE,step)],
                         getincln(excl3)[order(STATE,step)],
                         getincln(excl4)[order(STATE,step)],
                         getincln(excl5)[order(STATE,step)],
                         getincln(excl6)[order(STATE,step)]) |>
  mutate(excl_n = ifelse(step==0,0,lag(n) - n),
         excl_ind = ifelse(step==0,0,lag(ind)- ind)) 


write.csv(consort_n, paste(outpath,"consortn.csv"), row.names=FALSE)

### Create final cohorts

createcohort <- function(excl,num){
  #num <- as.numeric(gsub("\\D", "", deparse(substitute(excl))))
  
  cohort <- copy(included)[, sumexcl := rowSums(.SD[,excl, with=FALSE])
                           ][sumexcl==0][,.(link_id,mouddate,moud,STATE,YEAR)
                                         ][order(link_id,mouddate)]
  
  firstcohort <- cohort[cohort[, .I[1], by = link_id]$V1]
  
  saveRDS(firstcohort, paste0(path,"cohort",num,".rds"))
}

cohortlist <- list(excl1,excl2,excl3,excl4,excl5,excl6)
numlist <- seq(1:6)
walk2(cohortlist,numlist,createcohort)



