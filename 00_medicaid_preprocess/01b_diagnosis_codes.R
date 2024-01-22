############################################
############################################
# Create combined dx code file
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
# oslist <- paste0(list.files(inpath, pattern = "*other_services_header.*\\.parquet$", recursive = TRUE)) 
# os_head <- open_dataset(paste0(inpath, oslist), format="parquet", partition = "year") 
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
# os_head <- makelink(os_head)
# 
# 
# #################
# # Processing
# #################
# 
# # Make long file from numbered diagnosis codes
# makedxlong <- function(dat,tag){
#   dat |>
#     select("link_id","CLM_ID",
#            "SRVC_BGN_DT", "SRVC_END_DT",starts_with("DGNS_CD")) |>
#     mutate(SRVC_BGN_DT=ymd(SRVC_BGN_DT),
#            SRVC_END_DT=ymd(SRVC_END_DT)) |>
#     collect() |>
#     pivot_longer(cols=starts_with("DGNS_CD"),
#                  names_to = c("NUMBER"),
#                  names_prefix = "DGNS_CD_",
#                  values_to = "DGNS_CD") |>
#     filter(!is.na(DGNS_CD)) |>
#     mutate(source=tag, 
#            YEAR=year(SRVC_END_DT),
#            NUMBER=as.numeric(NUMBER))
# }
# 
# iplong <- makedxlong(ip_head,"ip")
# oslong <- makedxlong(os_head,"os")
# 
# print(paste0("rows in dx iplong ",nrow(iplong)))
# print(paste0("rows in dx oslong ",nrow(oslong)))
# 
# rm(iplist,oslist,os_head)
# 
# 
# # Also extract admitting dx code from IP
# ipadmit <- ip_head |>
#   select("link_id","CLM_ID",
#          "SRVC_BGN_DT", "SRVC_END_DT","ADMTG_DGNS_CD") |>
#   mutate(SRVC_BGN_DT=ymd(SRVC_BGN_DT),
#          SRVC_END_DT=ymd(SRVC_END_DT),
#          NUMBER=0, 
#          source="ip",
#          YEAR=year(SRVC_END_DT)) |>
#   rename(DGNS_CD=ADMTG_DGNS_CD) |>
#   filter(!is.na(DGNS_CD)) |> 
#   collect()
# print(paste0("rows in dx ipadmit ",nrow(ipadmit)))
# 
# rm(ip_head)
# 
# # Stack long files
# # merged <- rbind(iplong,ipadmit,oslong) 
# # print(paste0("rows in dx ",nrow(merged)))
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
# save_multiparquet(iplong,"dx_ip",5e6)
# save_multiparquet(oslong,"dx_os",5e6)
# save_multiparquet(ipadmit,"dx_ipa",5e6)
# toc()



###################################################################
############### Data.table
###################################################################

tic()
# IP
iplist <- paste0(list.files(inpath, pattern = "*inpatient_header.*\\.parquet$", recursive = TRUE))
ip_head <- open_dataset(paste0(inpath, iplist), format="parquet", partition = "year")

# OS
oslist <- paste0(list.files(inpath, pattern = "*other_services_header.*\\.parquet$", recursive = TRUE))
os_head <- open_dataset(paste0(inpath, oslist), format="parquet", partition = "year")

#################
# Make key id
#################

# makelink <- function(dat){
#   dat |>
#     mutate(link_id = ifelse(grepl("^\\s*$", BENE_ID)==TRUE,
#                             paste0(MSIS_ID,STATE_CD),
#                             paste0(BENE_ID,STATE_CD)))
# }

ip_head <- setDT(makelink(ip_head) |> 
                   select("link_id","CLM_ID","SRVC_BGN_DT","SRVC_END_DT",starts_with("DGNS_CD"),"ADMTG_DGNS_CD",
                          ADMSN_DT,DSCHRG_DT,starts_with("PRCDR_CD_DT_")) |>
                   collect() |>
                   mutate(SRVC_BGN_DT=fifelse(is.na(SRVC_BGN_DT),SRVC_END_DT,SRVC_BGN_DT),
                          maxdate = pmax(SRVC_BGN_DT,SRVC_END_DT,ADMSN_DT,DSCHRG_DT,
                                         PRCDR_CD_DT_1,PRCDR_CD_DT_2,PRCDR_CD_DT_3,PRCDR_CD_DT_4,PRCDR_CD_DT_5,PRCDR_CD_DT_6,
                                         na.rm = TRUE),
                          SRVC_END_DT=fifelse(SRVC_BGN_DT>SRVC_END_DT,maxdate,SRVC_END_DT)) |>
                 select("link_id","CLM_ID","SRVC_BGN_DT","SRVC_END_DT",starts_with("DGNS_CD"),"ADMTG_DGNS_CD"))

os_head <- setDT(makelink(os_head) |> 
                   select("link_id","CLM_ID","SRVC_BGN_DT","SRVC_END_DT",
                          starts_with("DGNS_CD")) |>
                   collect() |>
                   mutate(SRVC_BGN_DT=fifelse(is.na(SRVC_BGN_DT),SRVC_END_DT,SRVC_BGN_DT),
                          SRVC_END_DT=fifelse(SRVC_BGN_DT>SRVC_END_DT,SRVC_BGN_DT,SRVC_END_DT))) 
                   

#################
# Processing
#################

# Make long file from numbered diagnosis codes
makedxlong <- function(dat,tag){

  codevars <- names(dat)[starts_with("DGNS_CD", vars=names(dat))]
  #select_cols <- c("link_id","CLM_ID","SRVC_BGN_DT", "SRVC_END_DT",codevars)

  dat[, melt(.SD, measure.vars = list(codevars), value.name = c("DGNS_CD"), variable.factor=FALSE)
  ][!is.na(DGNS_CD)
  ][,  `:=` (NUMBER=parse_number(variable),
             variable=NULL,
             YEAR=year(SRVC_END_DT),
             source=tag)]
}

iplong <- makedxlong(ip_head,"ip") |> select(-ADMTG_DGNS_CD)
oslong <- makedxlong(os_head,"os")

print(paste0("rows in dx iplong ",nrow(iplong)))
print(paste0("rows in dx oslong ",nrow(oslong)))

rm(iplist,oslist,os_head)


# Also extract admitting dx code from IP
ipadmit <- ip_head[!is.na(ADMTG_DGNS_CD),c("link_id","CLM_ID","SRVC_BGN_DT", "SRVC_END_DT","ADMTG_DGNS_CD")
                   ][,  `:=` (DGNS_CD=ADMTG_DGNS_CD,
                              NUMBER=0,
                              ADMTG_DGNS_CD=NULL,
                              YEAR=year(SRVC_END_DT),
                              source="ip")]

print(paste0("rows in dx ipadmit ",nrow(ipadmit)))

rm(ip_head)


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

save_multiparquet(iplong,"dx_ip",5e6)
save_multiparquet(oslong,"dx_os",5e6)
save_multiparquet(ipadmit,"dx_ipa",5e6)
toc()




