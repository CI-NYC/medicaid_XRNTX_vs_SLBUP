############################################
############################################
# Create file of claims with ED visits
# Author: Rachael Ross
############################################
############################################

source("/home/rr3551/moudr01/scripts/00_preprocess/01_paths_fxs.R")

#################
# Libraries
#################
# library(arrow)
# library(tidyverse)
# library(lubridate)

#################
# Paths
#################

#Local/full
#inpath <- paste0(Sys.getenv("HOME"),"/07 Postdoc/02 Projects/99 Misc/Medicaid Resources/fakedata/data/")
#outpath <- paste0(Sys.getenv("HOME"),"/07 Postdoc/02 Projects/99 Misc/Medicaid Resources/fakedata/data/processed/")

#AWS/full
#inpath <- "/mnt/data/disabilityandpain-r/moud/parsed/12692/"
#outpath <- "/home/rr3551/moudr01/data/processed/"

#AWS/sample
#inpath <- "/home/rr3551/moudr01/data/sample/"
#outpath <- "/home/rr3551/moudr01/data/sample/processed/"

#################
# Load files
#################
tic()

# Code lists
ed_cpt <- read_csv(paste0(codes,"ed_cpt.csv")) |> mutate(code=str_pad(code, 5, pad="0"))
ed_rev <- read_csv(paste0(codes,"ed_rev.csv"))

# Procedure codes
list <- list.files(outpath, pattern = "*px_.*\\.parquet$", recursive = TRUE) 
pxs <- open_dataset(paste0(outpath, list), format="parquet") |>
  filter(PRCDR_CD %in% ed_cpt$code) |>
  select(link_id,CLM_ID) |>
  distinct() |>
  collect()

# Revenue codes
list <- list.files(outpath, pattern = "*rev_.*\\.parquet$", recursive = TRUE) 
revs <- open_dataset(paste0(outpath, list), format="parquet") |>
  filter(REV_CNTR_CD %in% ed_rev$code) |>
  select(link_id,CLM_ID) |>
  distinct() |>
  collect()

#################
# Processing 
#################

# Combine to get a unique list of all claims with ED visits
ed <- rbind(pxs,revs) |>
  distinct()

#################
# Save output
#################

save_multiparquet(ed,"ed",5e6)
toc()



