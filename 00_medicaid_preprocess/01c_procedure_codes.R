############################################
############################################
# Create combined procedure code file
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

# #################
# # Load files
# #################
# tic()
# # IP
# iplist <- paste0(list.files(inpath, pattern = "*inpatient_header.*\\.parquet$", recursive = TRUE))
# ip_head <- open_dataset(paste0(inpath, iplist), format="parquet", partition = "year")
# 
# # OS
# oslist <- paste0(list.files(inpath, pattern = "*other_services_line.*\\.parquet$", recursive = TRUE)) 
# os_line <- open_dataset(paste0(inpath, oslist), format="parquet", partition = "year") 
# 
# #################
# # Make key id
# #################
# 
# # makelink <- function(dat){
# #   dat |> 
# #     mutate(link_id = ifelse(grepl("^\\s*$", BENE_ID)==TRUE,
# #                             paste0(MSIS_ID,STATE_CD),
# #                             paste0(BENE_ID,STATE_CD)))
# # }
# 
# ip_head <- makelink(ip_head)
# os_line <- makelink(os_line)
# 
# #################
# # Processing
# #################
# 
# ### First deal with 6 px codes in inpatient header
# # Px code
# pxcode_long <- ip_head |>
#   select("link_id",SRVC_BGN_DT,SRVC_END_DT,"CLM_ID",PRCDR_CD_1,PRCDR_CD_2,
#          PRCDR_CD_3,PRCDR_CD_4,PRCDR_CD_5,PRCDR_CD_6) |>
#   collect() |>
#   pivot_longer(cols=starts_with("PRCDR_CD"),
#                names_to = c("NUM"),
#                names_prefix = "PRCDR_CD_",
#                values_to = "PRCDR_CD")
# 
# # Px date
# pxdt_long <- ip_head |>
#   select("link_id","CLM_ID",starts_with("PRCDR_CD_DT_")) |>
#   collect() |>
#   pivot_longer(cols=starts_with("PRCDR_CD_DT_"),
#                names_to = c("NUM"),
#                names_prefix = "PRCDR_CD_DT_",
#                values_to = "PRCDR_CD_DT")
# 
# # Px System
# pxsys_long <- ip_head |>
#   select("link_id","CLM_ID",starts_with("PRCDR_CD_SYS_")) |>
#   collect() |>
#   pivot_longer(cols=starts_with("PRCDR_CD_SYS_"),
#                names_to = c("NUM"),
#                names_prefix = "PRCDR_CD_SYS_",
#                values_to = "PRCDR_CD_SYS")
# 
# # Merge long ip files
# ip_px <- pxcode_long |>
#   left_join(pxdt_long, by=c("link_id"="link_id",
#                             "CLM_ID"="CLM_ID",
#                             "NUM"="NUM")) |>
#   left_join(pxsys_long, by=c("link_id"="link_id",
#                              "CLM_ID"="CLM_ID",
#                              "NUM"="NUM")) |>
#   mutate(NUM = as.numeric(factor(NUM)),
#          SRVC_BGN_DT=ymd(SRVC_BGN_DT),
#          SRVC_END_DT=ymd(SRVC_END_DT),
#          PRCDR_CD_DT=ymd(PRCDR_CD_DT),
#          source="ip",
#          YEAR=year(PRCDR_CD_DT)) |>
#   filter(!is.na(PRCDR_CD))
# print(paste0("rows in ip px ",nrow(ip_px)))
# 
# rm(iplist,ip_head,pxcode_long,pxdt_long,pxsys_long)
# 
# ### Now px code in other services line
# os_px <- os_line |>
#   select("link_id","CLM_ID","LINE_NUM",LINE_SRVC_BGN_DT,LINE_SRVC_END_DT,
#          starts_with("LINE_PRCDR_CD")) |>
#   filter(!is.na(LINE_PRCDR_CD)) |>
#   mutate(SRVC_BGN_DT=ymd(LINE_SRVC_BGN_DT),
#          SRVC_END_DT=ymd(LINE_SRVC_END_DT),
#          PRCDR_CD_DT=ymd(LINE_PRCDR_CD_DT),
#          NUM=as.numeric(LINE_NUM),
#          source="os",
#          YEAR=year(PRCDR_CD_DT)) |>
#   rename(PRCDR_CD=LINE_PRCDR_CD,
#          PRCDR_CD_SYS=LINE_PRCDR_CD_SYS) |>
#   select(-c(LINE_SRVC_BGN_DT,LINE_SRVC_END_DT,LINE_PRCDR_CD_DT,LINE_NUM)) |>
#   collect()
# print(paste0("rows in os px ",nrow(os_px)))
# 
# rm(oslist,os_line)
# 
# # Stack long files
# # merged <- rbind(ip_px,os_px) 
# # print(paste0("rows in px merged ",nrow(merged)))
# 
# #################
# # Save output
# #################
# 
# # # Partition into mutiple files as needed
# # save_multiparquet <- function(df,name,max_row){
# #   # If only 1 parquet needed
# #   if(nrow(df) <= max_row){
# #     write_parquet(df, 
# #                   paste0(outpath,name,"_0.parquet"))
# #   }
# #   
# #   # Multiple parquet files are needed
# #   if(nrow(df) > max_row){
# #     count = ceiling(nrow(df)/max_row)
# #     start = seq(1, count*max_row, by=max_row)
# #     end   = c(seq(max_row, nrow(df), max_row), nrow(df))
# #     
# #     for(j in 1:count){
# #       write_parquet(
# #         dplyr::slice(df, start[j]:end[j]), 
# #         paste0(outpath,name,"_",j-1,".parquet"))
# #     }
# #   }
# # }
# 
# save_multiparquet(ip_px,"px_ip",5e6)
# save_multiparquet(os_px,"px_os",5e6)
# toc()
# 



###################################################################
############### Data.table
###################################################################

#################
# Load files
#################
tic()
# IP
iplist <- paste0(list.files(inpath, pattern = "*inpatient_header.*\\.parquet$", recursive = TRUE))
ip_head <- open_dataset(paste0(inpath, iplist), format="parquet", partition = "year")

# OS
oslist <- paste0(list.files(inpath, pattern = "*other_services_line.*\\.parquet$", recursive = TRUE)) 
os_line <- open_dataset(paste0(inpath, oslist), format="parquet", partition = "year") 

#################
# Make key id
#################

# makelink <- function(dat){
#   dat |> 
#     mutate(link_id = ifelse(grepl("^\\s*$", BENE_ID)==TRUE,
#                             paste0(MSIS_ID,STATE_CD),
#                             paste0(BENE_ID,STATE_CD)))
# }

tic()
CD <- paste("PRCDR_CD_", 1:6, sep = "")
DT <- paste("PRCDR_CD_DT_", 1:6, sep = "")
SYS <- paste("PRCDR_CD_SYS_", 1:6, sep = "")
ip_head <- setDT(makelink(ip_head) |>
                   select("link_id",SRVC_BGN_DT,SRVC_END_DT,"CLM_ID",all_of(CD),all_of(DT),all_of(SYS)) |> 
                   filter(!is.na(PRCDR_CD_1)) |>
                   collect())

os_line <- setDT(makelink(os_line) |>
                   select("link_id","CLM_ID","LINE_NUM",LINE_SRVC_BGN_DT,LINE_SRVC_END_DT,
                          starts_with("LINE_PRCDR_CD")) |>
                   filter(!is.na(LINE_PRCDR_CD)) |>
                   collect())

#################
# Processing
#################

### First deal with 6 px codes in inpatient header
ip_px <- ip_head[, melt(.SD, 
                         measure.vars = list(CD,DT,SYS), 
                         value.name = c("PRCDR_CD", "PRCDR_CD_DT", "PRCDR_CD_SYS"), 
                         variable.factor=FALSE)
                  ][!is.na(PRCDR_CD)
                    ][, `:=` (NUM=as.numeric(variable),
                              variable=NULL,
                              source="ip")
                      ][,PRCDR_CD_DT := fifelse(is.na(PRCDR_CD_DT),SRVC_BGN_DT,
                                                fifelse(PRCDR_CD_DT<SRVC_BGN_DT,SRVC_BGN_DT,PRCDR_CD_DT)) #if date missing replace with service begin date
                        ][,YEAR := year(PRCDR_CD_DT)]

print(paste0("rows in ip px ",nrow(ip_px)))

rm(iplist,ip_head)


### Now px code in other services line

os_px <- os_line[, `:=` (NUM=as.numeric(LINE_NUM),
                             LINE_NUM=NULL,
                             source="os",
                             PRCDR_CD=LINE_PRCDR_CD,
                             LINE_PRCDR_CD=NULL,
                             PRCDR_CD_DT=LINE_PRCDR_CD_DT,
                             LINE_PRCDR_CD_DT=NULL,
                             PRCDR_CD_SYS=LINE_PRCDR_CD_SYS,
                             LINE_PRCDR_CD_SYS=NULL,
                             SRVC_END_DT=LINE_SRVC_END_DT,
                             LINE_SRVC_END_DT=NULL,
                             SRVC_BGN_DT=LINE_SRVC_BGN_DT,
                             LINE_SRVC_BGN_DT=NULL)
                 ][,PRCDR_CD_DT := fifelse(is.na(PRCDR_CD_DT),SRVC_BGN_DT,PRCDR_CD_DT) #if date missing replace with service begin date
                   ][,YEAR := year(PRCDR_CD_DT)]
print(paste0("rows in os px ",nrow(os_px)))

rm(oslist,os_line)

# Stack long files
# merged <- rbind(ip_px,os_px) 
# print(paste0("rows in px merged ",nrow(merged)))

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

save_multiparquet(ip_px,"px_ip",5e6)
save_multiparquet(os_px,"px_os",5e6)
toc()
