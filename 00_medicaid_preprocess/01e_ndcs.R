############################################
############################################
# Create combined NDC file
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
# inpath <- "/home/rr3551/moudr01/data/sample/"
# outpath <- "/home/rr3551/moudr01/data/sample/processed/"

#################
# Load files
#################
tic()
# IP
iplist <- paste0(list.files(inpath, pattern = "*inpatient_line.*\\.parquet$", recursive = TRUE))
ip_line <- open_dataset(paste0(inpath, iplist), format="parquet", partition = "year")

# OS
oslist <- paste0(list.files(inpath, pattern = "*other_services_line.*\\.parquet$", recursive = TRUE)) 
os_line <- open_dataset(paste0(inpath, oslist), format="parquet", partition = "year") 

# RX
rxlist<- paste0(list.files(inpath, pattern = "*rx_line.*\\.parquet$", recursive = TRUE)) 
rx_line <- open_dataset(paste0(inpath, rxlist), format="parquet", partition = "year") 


#################
# Make key id
#################

# makelink <- function(dat){
#   dat |> 
#     mutate(link_id = ifelse(grepl("^\\s*$", BENE_ID)==TRUE,
#                             paste0(MSIS_ID,STATE_CD),
#                             paste0(BENE_ID,STATE_CD)))
# }

ip_line <- makelink(ip_line)
os_line <- makelink(os_line)
rx_line <- makelink(rx_line)

#################
# Processing
#################

# First process inpatient and other services
processndc <- function(dat,tag){
  dat |>
    select("link_id","CLM_ID","LINE_NUM",LINE_SRVC_BGN_DT,LINE_SRVC_END_DT,
           NDC,NDC_UOM_CD, NDC_QTY) |>
    mutate(SRVC_BGN_DT=ymd(LINE_SRVC_BGN_DT),
           SRVC_END_DT=ymd(LINE_SRVC_END_DT),
           NUM=as.numeric(LINE_NUM),
           source=tag,
           YEAR=year(LINE_SRVC_END_DT),
           DAYS_SUPPLY=NA,
           DOSAGE_FORM_CD=NA,
           RX_FILL_DT=SRVC_END_DT) |>
    select(-c(LINE_SRVC_BGN_DT,LINE_SRVC_END_DT,LINE_NUM)) |>
    filter(!is.na(NDC)) |>
    collect() |>
    mutate(SRVC_BGN_DT=fifelse(is.na(SRVC_BGN_DT),SRVC_END_DT,SRVC_BGN_DT),
           SRVC_END_DT=fifelse(SRVC_BGN_DT>SRVC_END_DT,SRVC_BGN_DT,SRVC_END_DT))
}

ipndc <- processndc(ip_line,"ip")
osndc <- processndc(os_line,"os")

print(paste0("rows in ndc ipndc ",nrow(ipndc)))
print(paste0("rows in ndc osndc ",nrow(osndc)))

# First process rx table
rxndc <- rx_line |>
  select("link_id","CLM_ID","LINE_NUM",RX_FILL_DT,
         NDC,NDC_UOM_CD, NDC_QTY,DAYS_SUPPLY,DOSAGE_FORM_CD) |>
  mutate(RX_FILL_DT=ymd(RX_FILL_DT),
         SRVC_BGN_DT=RX_FILL_DT,
         SRVC_END_DT=RX_FILL_DT,
         NUM=as.numeric(LINE_NUM),
         source="rx",
         YEAR=year(RX_FILL_DT)) |>
  select(-c(LINE_NUM)) |>
  filter(!is.na(NDC)) |>
  collect()

print(paste0("rows in ndc rxndc ",nrow(rxndc)))

rm(iplist,ip_line,oslist,os_line,rxlist,rx_line)

# Stack files
# merged <- rbind(rxndc,ipndc,osndc) 
# print(paste0("rows in ndc merged ",nrow(merged)))

#################
# Save output
#################

# # Partition into mutiple files as needed
# save_multiparquet <- function(df,name,max_row){
#   # If only 1 parquet needed
#   if(nrow(df) <= max_row){
#     write_parquet(df, 
#                   paste0(outpath,name,"_0.parquet"))
#   }
#   
#   # Multiple parquet files are needed
#   if(nrow(df) > max_row){
#     count = ceiling(nrow(df)/max_row)
#     start = seq(1, count*max_row, by=max_row)
#     end   = c(seq(max_row, nrow(df), max_row), nrow(df))
#     
#     for(j in 1:count){
#       write_parquet(
#         dplyr::slice(df, start[j]:end[j]), 
#         paste0(outpath,name,"_",j-1,".parquet"))
#     }
#   }
# }

save_multiparquet(ipndc,"ndc_ip",5e6)
save_multiparquet(osndc,"ndc_os",5e6)
save_multiparquet(rxndc,"ndc_rx",5e6)
toc()