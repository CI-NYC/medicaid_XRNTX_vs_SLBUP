############################################
############################################
# Pulling all MOUD fills/procedures
# Author: Rachael Ross
#
# Output: moud_ndc_raw = occurence of moud ndcs
#         moud_px_raw = occurence of moud pxs
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

# Procedure data
list <- list.files(inproc, pattern = "*px_*", recursive = TRUE) 
px_df <- open_dataset(paste0(inproc, list), format="parquet")

# ndc data
list <- list.files(inproc, pattern = "*ndc_*", recursive = TRUE)
ndc_df <- open_dataset(paste0(inproc, rev(list)), format="parquet")

# Code lists
ndclist <- c("bup_ndc.csv","ntx_ndc.csv")
icdpxlist <- c("meth_icdpx.csv")
hcpcslist <- c("bup_hcpcs.csv","meth_hcpcs.csv","ntx_hcpcs.csv")
cptlist <- c("bup_cpt.csv","ntx_cpt.csv")

ndc_codes <- map_df(paste0(codes,ndclist), read.csv) |>
  mutate(code=str_pad(code, 11, pad="0")) |> distinct()

icdpx_codes <- map_df(paste0(codes,icdpxlist), read.csv) |> distinct()

hcpc_codes <- map_df(paste0(codes,hcpcslist), read.csv) |> distinct()

cpt_codes <- map_df(paste0(codes,cptlist), read.csv) |>
  mutate(code=str_pad(code, 5, pad="0")) |> distinct()

# OUD diagnoses
list <- list.files(outpath, pattern = "*oud_dx_*", recursive = TRUE)
oud_df <- open_dataset(paste0(outpath, list), format="parquet") |> collect()


#################
# Extract fills/procedures
#################

# NDCs
moud_ndc <- ndc_df |>
  filter(NDC %in% ndc_codes$code & YEAR %in% c(2016,2017,2018,2019)) |>
  filter(is.na(DAYS_SUPPLY)|DAYS_SUPPLY>0) |> #check max in full data
  left_join(ndc_codes, by = c("NDC" = "code")) |>
  mutate(ouddx1 = 0, with_ouddx = 0) |> #create var of all zeros to be used later
  collect()

# Procedures
getpx <- function(sys,codelist){
  px_df |>
    filter(PRCDR_CD_SYS==sys, !is.na(PRCDR_CD_DT)) |>
    filter(PRCDR_CD %in% codelist$code  & YEAR %in% c(2016,2017,2018,2019)) |>
    left_join(codelist, by = c("PRCDR_CD" = "code")) |>
    collect() 
}

moud_icdpx <- getpx("07",icdpx_codes) |> 
  mutate(ouddx1=0,with_ouddx=0) #create var of all zeros to be used later

moud_hcpc_ <- getpx("06",hcpc_codes)
moud_cpt_ <- getpx("01",cpt_codes)


moud_cpt_ |> filter(link_id=="qqqqqqJpCq8DCJjCA")
# Assess oud dx on the same date for hcpcs and cpts
setDT(oud_df)
setkey(oud_df,link_id,SRVC_BGN_DT,SRVC_END_DT) 

assessoud <- function(dat){
  DT <- setDT(dat |> select(link_id,PRCDR_CD_DT) |> distinct() |> mutate(dt2=PRCDR_CD_DT))
  withdx <-  foverlaps(DT, oud_df, 
                       by.x = c("link_id", "PRCDR_CD_DT", "dt2"),
                       by.y = c("link_id", "SRVC_BGN_DT", "SRVC_END_DT"),
                       type = "within", mult = "first", nomatch=NULL)[,with_ouddx := 0
                       ][,.(link_id,PRCDR_CD_DT,with_ouddx)]
  
  # Merge flag into file
  dat |>
    left_join(withdx, by=c("link_id","PRCDR_CD_DT")) |>
    mutate(with_ouddx = ifelse(is.na(with_ouddx),0,1))
}

moud_cpt <- assessoud(moud_cpt_)
moud_hcpc <- assessoud(moud_hcpc_)

# Combine moud procedures into one file, filter on with oud dx if required
moud_px <- rbind(moud_icdpx,moud_hcpc,moud_cpt) |>
  filter(ouddx1==0|(ouddx1==1 & with_ouddx==1))

#################
# Save output
#################

write_parquet(moud_ndc,paste0(outpath,"moud_ndc_raw.parquet"))
write_parquet(moud_px,paste0(outpath,"moud_px_raw.parquet"))

print(paste0(nrow(moud_ndc),"in moud_ndc"))
print(paste0(nrow(moud_px),"in moud_px"))
toc()
