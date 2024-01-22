############################################
############################################
# Create long monthly enrollment file
# Author: Rachael Ross
############################################
############################################

source("/home/rr3551/moudr01/scripts/00_preprocess/01_paths_fxs.R")

#################
# Libraries
#################
# library(arrow)
# library(tidyverse)

#################
# Paths
#################

#Local/full
#inpath <- paste0(Sys.getenv("HOME"),"/07 Postdoc/02 Projects/99 Misc/Medicaid Resources/fakedata/data/")
#outpath <- paste0(Sys.getenv("HOME"),"/07 Postdoc/02 Projects/99 Misc/Medicaid Resources/fakedata/data/processed/")

#AWS/full
#inpath <- "/mnt/data/disabilityandpain-r/moud/parsed/12692/"
#outpath <- "/home/rr3551/moudr01/data/processed/"

# #AWS/sample
# inpath <- "/home/rr3551/moudr01/data/sample/"
# outpath <- "/home/rr3551/moudr01/data/sample/processed/"

#################
# Load files
#################
tic()
filelist <- list.files(inpath, pattern = "*base.*\\.parquet$", recursive = TRUE) 
de_base <- open_dataset(paste0(inpath, filelist), format="parquet", partition = "year") 

#################
# Make ids
#################
de_base <- de_base |> 
  mutate(link_id = ifelse(grepl("^\\s*$", BENE_ID)==TRUE|is.na(BENE_ID),
                         paste0(MSIS_ID,STATE_CD),
                         paste0(BENE_ID,STATE_CD)),
         unique_id = paste0(BENE_ID,MSIS_ID,STATE_CD,RFRNC_YR)) |>
  select("unique_id","link_id","BENE_ID","STATE_CD","RFRNC_YR",
         starts_with("MDCD_ENRLMT_DAYS"),-c("MDCD_ENRLMT_DAYS_YR"), 
         starts_with("DUAL_ELGBL_CD"),-c("DUAL_ELGBL_CD_LTST"),
         starts_with("RSTRCTD_BNFTS_CD"),-c("RSTRCTD_BNFTS_CD_LTST")) |>
  collect()


# # Drop duplicates entirely
# nodups <- de_base |>
#   select(key_id) |>
#   group_by(key_id) |>
#   summarise(cnt = n()) |>
#   filter(cnt==1) |>
#   select(key_id) 

#################
# Processing
#################

### Make data long
days <- names(de_base)[starts_with("MDCD_ENRLMT_DAYS", vars=names(de_base))]
dual <- names(de_base)[starts_with("DUAL_ELGBL_CD", vars=names(de_base))]
bnft <- names(de_base)[starts_with("RSTRCTD_BNFTS_CD", vars=names(de_base))]


long <- setDT(de_base)[, melt(.SD, 
                        measure.vars = list(days,dual,bnft), 
                        value.name = c("MDCD_ENRLMT_DAYS", "DUAL_ELGBL_CD", "RSTRCTD_BNFTS_CD"), 
                        variable.factor=FALSE)
                ][, `:=` (MONTH=as.numeric(variable),
                          variable=NULL,
                          YEAR=as.numeric(RFRNC_YR),
                          MDCD_ENRLMT_DAYS = ifelse(is.na(MDCD_ENRLMT_DAYS),0,MDCD_ENRLMT_DAYS),
                          nodual_flag = ifelse(grepl("^\\s*$", DUAL_ELGBL_CD)==TRUE,NA,
                                               ifelse(DUAL_ELGBL_CD=="00",1,0)),
                          fullbnfts_flag = ifelse(grepl("^\\s*$", RSTRCTD_BNFTS_CD)==TRUE,NA,
                                                  ifelse(RSTRCTD_BNFTS_CD=="1",1,0)))]


# # Enrollment days
# mdcddys_long <- de_base |>
#   select("unique_id","link_id","BENE_ID","STATE_CD","RFRNC_YR",
#          starts_with("MDCD_ENRLMT_DAYS"),-c("MDCD_ENRLMT_DAYS_YR")) |>
#   collect() |>
#   pivot_longer(cols=starts_with("MDCD_ENRLMT_DAYS"),
#                names_to = c("MONTH"),
#                names_prefix = "MDCD_ENRLMT_DAYS_",
#                values_to = "MDCD_ENRLMT_DAYS") 
#              
# # Dual enrollment
# dual_long <- de_base |>
#   select("unique_id",
#          starts_with("DUAL_ELGBL_CD"),-c("DUAL_ELGBL_CD_LTST")) |>
#   collect() |>
#   pivot_longer(cols=starts_with("DUAL_ELGBL_CD"),
#                names_to = c("MONTH"),
#                names_prefix = "DUAL_ELGBL_CD_",
#                values_to = "DUAL_ELGBL_CD") 
# 
# # Restricted benefits
# rstrctd_long <- de_base |>
#   select("unique_id",
#          starts_with("RSTRCTD_BNFTS_CD"),-c("RSTRCTD_BNFTS_CD_LTST")) |>
#   collect() |>
#   pivot_longer(cols=starts_with("RSTRCTD_BNFTS_CD"),
#                names_to = c("MONTH"),
#                names_prefix = "RSTRCTD_BNFTS_CD_",
#                values_to = "RSTRCTD_BNFTS_CD") 
# 
 rm(filelist,de_base)
# 
# # Merge together
# merged <- mdcddys_long |>
#   left_join(dual_long, by=c("unique_id"="unique_id",
#                             "MONTH"="MONTH")) |>
#   left_join(rstrctd_long, by=c("unique_id"="unique_id",
#                           "MONTH"="MONTH")) |>
#   mutate(MDCD_ENRLMT_DAYS = ifelse(is.na(MDCD_ENRLMT_DAYS),0,MDCD_ENRLMT_DAYS),
#          nodual_flag = ifelse(grepl("^\\s*$", DUAL_ELGBL_CD)==TRUE,NA,
#                               ifelse(DUAL_ELGBL_CD=="00",1,0)),
#          fullbnfts_flag = ifelse(grepl("^\\s*$", RSTRCTD_BNFTS_CD)==TRUE,NA,
#                                  ifelse(RSTRCTD_BNFTS_CD=="1",1,0)),
#          YEAR=as.numeric(RFRNC_YR),
#          MONTH = as.numeric(factor(MONTH)))
# 
# rm(mdcddys_long,rstrctd_long,dual_long)

# Dedup by taking max days, min of flags

# tic()
# nodups <- merged |>
#   group_by(link_id, BENE_ID, STATE_CD, RFRNC_YR, MONTH, YEAR) |>
#   #mutate(num=cur_group_id()) |>
#   summarise(MDCD_ENRLMT_DAYS = max(MDCD_ENRLMT_DAYS, na.rm = TRUE),
#             nodual_flag = ifelse(is.infinite(min(nodual_flag, na.rm = TRUE)),
#                                  NA, min(nodual_flag, na.rm = TRUE)),
#             fullbnfts_flag = ifelse(is.infinite(min(fullbnfts_flag, na.rm = TRUE)),
#                                     NA, min(fullbnfts_flag, na.rm = TRUE))) |>
#   mutate(YEAR=as.numeric(RFRNC_YR))
# toc() #<3188.9 sec (53min) (5% 133sec)
# 
# tic()
# nodups <- sqldf("SELECT link_id, BENE_ID, STATE_CD, RFRNC_YR, MONTH, YEAR,
#                  max(MDCD_ENRLMT_DAYS) as MDCD_ENRLMT_DAYS, 
#                  min(nodual_flag) as nodual_flag,
#                  min(fullbnfts_flag) as fullbnfts_flag
#                  FROM long
#                  group by link_id, BENE_ID, STATE_CD, RFRNC_YR, MONTH, YEAR
#                 order by link_id, YEAR, MONTH")
# toc() #498 sec


nodups <- copy(long)[,.(MDCD_ENRLMT_DAYS = max(MDCD_ENRLMT_DAYS, na.rm = TRUE),
                      nodual_flag = min(nodual_flag, na.rm = TRUE),
                      fullbnfts_flag = min(fullbnfts_flag, na.rm = TRUE)), 
                                by=.(link_id, BENE_ID, STATE_CD, RFRNC_YR, MONTH, YEAR)
        ][, `:=` (MDCD_ENRLMT_DAYS = ifelse(is.infinite(MDCD_ENRLMT_DAYS),NA,MDCD_ENRLMT_DAYS),
                  nodual_flag = ifelse(is.infinite(nodual_flag),NA,nodual_flag),
                  fullbnfts_flag = ifelse(is.infinite(fullbnfts_flag),NA,fullbnfts_flag))
          ][order(link_id, YEAR, MONTH)]



#rm(merged)
print(paste0("rows in monthly_enroll ",nrow(nodups)))

#################
# Save output
#################
           

# For an unknown reason, calling this function is not working with full data

# name <- "monthly_enroll"
# max_row <- 5e6
# df <- nodups
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

save_multiparquet(nodups,"monthly_enroll",5e6)
toc()


