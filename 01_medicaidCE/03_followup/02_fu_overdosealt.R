############################################
############################################
# Identify overdose during follow-up using alternative definition
# Author: Rachael Ross
#
# Output: fu_overalt
############################################
############################################

source("/home/rr3551/moudr01/scripts/01_medicaidCE/00_paths_fxs.R")

#################
# Load files
#################
tic()

# Code lists
overdose_dx_alt <- read_csv(paste0(codes,"overdose_dx_alt.csv"))

# Table of unique MOUD initiate dates
list <- list.files(outpath, pattern = "*imoud_dates*", recursive = TRUE) 
imoud_dates <- open_dataset(paste0(outpath, list), format="parquet") |> 
  select(-moud) |>
  collect() |>
  distinct() |>
  mutate(fudt_studyend = update(today(),month=12,year=2019,day=31))
setDT(imoud_dates)  

# Diagnosis codes
list <- list.files(inproc, pattern = "*dx_.*\\.parquet$", recursive = TRUE) 
dxs <- open_dataset(paste0(inproc, list), format="parquet") |>
  filter(link_id %in% imoud_dates$link_id) |> 
  filter(DGNS_CD %in% overdose_dx_alt$code) |>
  collect() 


#################
# Processing
#################

### Overdose
setDT(dxs)
setkey(dxs,link_id,SRVC_BGN_DT,SRVC_END_DT) 

withdx <-  foverlaps(imoud_dates, dxs, 
                     by.x = c("link_id", "mouddate", "fudt_studyend"),
                     by.y = c("link_id", "SRVC_BGN_DT", "SRVC_END_DT"),
                     type = "any", mult = "all", nomatch=NULL)

overdosefu <- withdx[SRVC_BGN_DT>mouddate, #only services dates that occur after mouddate
                      .(fudt_overdosealt = min(SRVC_BGN_DT)), #which date to use here?
                      by=.(link_id,mouddate)] 

### Merge
with_fudates <- imoud_dates |>
  left_join(overdosefu, by=c("link_id","mouddate")) |>
  mutate(fudays_overdosealt = fudt_overdosealt - mouddate) |>
  mutate(excl_earlyoverdosealt = case_when(fudays_overdosealt <= earlywindow ~ 1, .default=0))

forfu <- with_fudates |>
  select(link_id,mouddate,fudt_overdosealt,fudays_overdosealt,excl_earlyoverdosealt)


#################
# Save output
#################

saveRDS(forfu,paste0(outpath,"fu_overalt.rds"))
print(paste0(nrow(forfu),"in fu_overalt"))
toc()