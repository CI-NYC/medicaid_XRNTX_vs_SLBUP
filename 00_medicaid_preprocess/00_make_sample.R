############################################
############################################
# Create a sample of the medicaid data
# Author: Rachael Ross
############################################
############################################

#################
# Libraries
#################
library(arrow)
library(tidyverse)
library(tictoc)
library(dbplyr)

#################
# Path
#################

inpath <- paste0("/mnt/processed-data/moud","/parsed/12692/")
outpath <- "/home/data/moud/global/sample/raw/"

#################
# Load files
#################

filelist <- list.files(inpath, pattern = "*base.*\\.parquet$", recursive = TRUE) 
de_base <- open_dataset(paste0(inpath, filelist), format="parquet")

years <- list.dirs(inpath, full.names = FALSE)[nchar(list.dirs(inpath, full.names = FALSE)) == 4]

#################
# Make link id
#################

ids <- de_base |> 
  mutate(link_id = ifelse(grepl("^\\s*$", BENE_ID)==TRUE,
                          paste0(MSIS_ID,STATE_CD),
                          paste0(BENE_ID,STATE_CD))) |>
  select(link_id) |>
  distinct() |>
  collect()
         
#################
# Sample 
#################

set.seed(7) 
takesamp <- ids |> 
  #group_by(STATE_CD) |>
  slice_sample(prop=.02)

saveRDS(takesamp, file = paste0(outpath,"sample.rds"))

# Link to all the other files
tosample <- function(file,yr){
  dat <- open_dataset(paste0(inpath,yr,"/",file), 
                      format="parquet") |>
    mutate(link_id = ifelse(grepl("^\\s*$", BENE_ID)==TRUE,
                            paste0(MSIS_ID,STATE_CD),
                            paste0(BENE_ID,STATE_CD)))
  
  sampled <- dat |>
    inner_join(takesamp, by = c("link_id")) |>
    select(-c("link_id")) |>
    collect()
  
  write_parquet(sampled, 
                paste0(outpath,yr,"/", 
                       file,".parquet"))
}

tosample1 <- function(yr){
  allfiles <- list.files(paste0(inpath,yr))
  walk(allfiles, tosample, yr=yr)
}

walk(years, tosample1)


# # Loop option
# for (i in 1:length(years)) {
#   
#   #Loop through years
#   yr <- years[i]
#   
#   #All files in that year
#   allfiles <- list.files(paste0(inpath,yr))
# 
#   #Loop through all files
#   for (j in length(allfiles)){
#     
#     
#     dat <- open_dataset(paste0(inpath,yr,"/",allfiles[j]), 
#                         format="parquet")
#     sampled <- dat |>
#       inner_join(takesamp, by = c("MSIS_ID","STATE_CD"))
#     
#     write_parquet(sampled, 
#                   paste0(outpath,yr,"/", 
#                          allfiles[j]))
#   }
# }

