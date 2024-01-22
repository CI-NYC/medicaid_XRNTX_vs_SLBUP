############################################
############################################
# Obtain unique NDC list for all MOUDR01 data
# Author: Rachael Ross
############################################
############################################

# Libraries
library(arrow)
library(tidyverse)
library(lubridate)
library(tictoc)
library(sqldf)
library(data.table)

# Path
datapath <- "/home/data/moud/global/"

# # Load files
# list <- list.files(inproc, pattern = "*ndc_*", recursive = TRUE)
# ndc_df <- setDT(open_dataset(paste0(inproc, rev(list)), format="parquet") |>
#   select(NDC) |> collect())
# 
# #Get unique list
# unique_ndc <- unique(ndc_df)
# 
# #Save RDS of unique list
# saveRDS(unique_ndc,paste0(outpath,"unique_ndc.rds"))
# 
# #Load unique list (if previously created)
# unique_ndc <- readRDS(paste0(outpath,"unique_ndc.rds"))

#Load disability grant crosswalk
crosswalk <- readRDS(paste0(datapath,"ndc_atc_crosswalk/NDC_to_ATC_crosswalk.rds"))
crosswalk_df <- as.data.frame(cbind("NDC"=crosswalk$NDC,"atc"=crosswalk$atc))
#crosswalk_df <- as.data.frame(cbind("NDC"=crosswalk$NDC,"atc"=map_chr(crosswalk$atc,1)))

# 
# matched <- unique_ndc |>
#   filter(NDC %in% crosswalk$NDC)
# 
# #Find unmatched
# notmatched <- unique_ndc |>
#   filter(!(NDC %in% crosswalk$NDC))
# 
# saveRDS(notmatched,paste0(outpath,"more_ndcs.rds"))

#Nick Williams ran the more_ndcs.rds through this rxnorm package to create a 2nd crosswalk (NDC_to_ATC_crosswalk2.rds)
crosswalk2 <- readRDS(paste0(datapath,"ndc_atc_crosswalk/NDC_to_ATC_crosswalk2.rds"))
#found <- as.data.frame(crosswalk2[[1]])
#missing <- as.data.frame(crosswalk2[[2]])
#alien <- as.data.frame(crosswalk2[[3]])
#unknown <- as.data.frame(crosswalk2[[4]])

#Create full cross combining the two
found <- crosswalk2[[1]]
#found_df <- as.data.frame(cbind("NDC"=found$NDC,"atc"=map_chr(found$atc,1)))
found_df <- as.data.frame(cbind("NDC"=found$NDC,"atc"=found$atc))

#Check to see if any dups
anydups <- crosswalk_df |>
  inner_join(found_df, by=c("NDC"))


NDC_to_ATC_crosswalk_full <- rbind(crosswalk_df,found_df) |>
  mutate(NDC = unlist(NDC))

#inmoudr01 <- NDC_to_ATC_crosswalk_full |>
#  filter(NDC %in% unique_ndc$NDC)
# 75% of unique ndcs in moudr01 data were matched to an atc code by rxnorm

saveRDS(NDC_to_ATC_crosswalk_full,paste0(datapath,"ndc_atc_crosswalk/NDC_to_ATC_crosswalk_full.rds"))

