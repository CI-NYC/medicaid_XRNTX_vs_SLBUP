############################################
############################################
# Create cohort for sensitivity analysis
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

# Also load base_moud file
base_moud <- readRDS(paste0(path, "base_moud.rds")) |>
  filter(moud=="bup") |>
  select(-moud)

inclexcl_df <- imoud_dates |>
  left_join(iemoud_df, by=c("link_id","mouddate","moud")) |>
  left_join(ie_df0, by=c("link_id","mouddate")) |>
  left_join(ie_df1, by=c("link_id","mouddate")) |>
  left_join(ie_df2, by=c("link_id","mouddate")) |>
  left_join(ie_df3, by=c("link_id","mouddate")) |>
  left_join(base_moud, by=c("link_id","mouddate")) |>
  filter(moud != "mth") |>
  mutate(excl_tooshort = case_when(moud=="ntx" ~ 0,
                                   moud_tot_days <= 7 ~ 1,
                                   .default = 0))

### Get numbers for flowchart

excl2 <- c("excl_nocontenr",
           "excl_prioruse_alt","excl_multiday0", 
           "excl_tooshort",
           "excl_noouddx",
           "excl_preg","excl_cancer","excl_pall")

included <- inclexcl_df |> filter(incl_age==1)
setDT(included)

### Create final cohorts

createcohort <- function(excl,num){
  #num <- as.numeric(gsub("\\D", "", deparse(substitute(excl))))
  
  cohort <- copy(included)[, sumexcl := rowSums(.SD[,excl, with=FALSE])
                           ][sumexcl==0][,.(link_id,mouddate,moud,STATE,YEAR)
                                         ][order(link_id,mouddate)]
  
  firstcohort <- cohort[cohort[, .I[1], by = link_id]$V1]
  
  saveRDS(firstcohort, paste0(path,"cohort",num,".rds"))
}

# cohortlist <- list(excl1,excl2,excl3,excl4,excl5,excl6)
# numlist <- seq(1:6)
# walk2(cohortlist,numlist,createcohort)

createcohort(excl2,7)



