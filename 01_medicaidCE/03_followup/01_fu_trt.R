############################################-
############################################-
# Create treatment discontinuation variables
# Author: Rachael Ross
#
# Output: fu_trt
############################################-
############################################-

source("/home/rr3551/moudr01/scripts/01_medicaidCE/00_paths_fxs.R")

#################-
# Load files----
#################-
tic()

# Load episodes
episodes_indiv <- open_dataset(paste0(outpath, "episodes_indiv.parquet"), format="parquet") |>
  collect()
episodes_any <- open_dataset(paste0(outpath, "episodes_any.parquet"), format="parquet") |>
  collect()
episodes_bup <- open_dataset(paste0(outpath, "episodes_bup.parquet"), format="parquet") |>
  collect()
episodes_any_alt <- open_dataset(paste0(outpath, "episodes_any_alt.parquet"), format="parquet") |>
  collect()


#################-
# Set keys----
#################-

setkey(episodes_indiv,link_id,trt,startdt,endgracedt)
setkey(episodes_bup,link_id,startdt,endgracedt) 
setkey(episodes_any,link_id,startdt,endgracedt)
setkey(episodes_any_alt,link_id,startdt,endgracedt)

#################-
# Get discontinue dates----
#################-

num <- 2

getdts <- function(num){
  
  cohort <- readRDS(paste0(outpath,"cohort",num,".rds")) |> 
    mutate(end = mouddate + days(180)) |>
    as.data.table()
  
    # Definition a - discontinue all MOUD
  any <-  foverlaps(cohort, episodes_any,
                    by.x = c("link_id", "mouddate", "end"),
                    by.y = c("link_id", "startdt", "endgracedt"),
                    type = "any", mult = "first", nomatch=NA) |> #not first bc bup/mth pre ntx
    mutate(fudt_discona = endgracedt,
           fudays_discona = fudt_discona - mouddate) |>
    select(link_id,mouddate,moud,starts_with("fu"))
  
  print(paste("missing in a: ",nrow(any |> filter(is.na(fudt_discona)))))
  
  
  # Definition c - discontinue initiated MOUD including route
  specific <-  foverlaps(cohort, episodes_indiv |> filter(initiate==1),
                         by.x = c("link_id","moud","mouddate", "end"),
                         by.y = c("link_id","trt","startdt", "endgracedt"),
                         type = "any", mult = "first", nomatch=NA) |> #using first here/switch to any bc long bup
    mutate(fudt_disconc = endgracedt,
           fudays_disconc = fudt_disconc - mouddate) |>
    select(link_id,mouddate,moud,starts_with("fu"))
  
  print(paste("missing in c: ",nrow(specific |> filter(is.na(fudt_disconc)))))
  
  # Definition b - allowed to switch route - relevant only for bup
  bups <-  foverlaps(cohort |> filter(moud=="bup"), episodes_bup,
                     by.x = c("link_id","mouddate", "end"),
                     by.y = c("link_id","startdt", "endgracedt"),
                     type = "any", mult = "first", nomatch=NA) |> #using first here/switch to any bc long bup
    mutate(fudt_disconb = endgracedt) |>
    select(link_id,mouddate,moud,starts_with("fu"))
  
  print(paste("missing in b: ",nrow(bups |> filter(is.na(fudt_disconb)))))
  
  # WITH ALTERNATIVE GRACE PERIOD Definition a - discontinue all MOUD - WITH ALTERNATIVE GRACE PERIOD
  any_alt <-  foverlaps(cohort, episodes_any_alt,
                    by.x = c("link_id", "mouddate", "end"),
                    by.y = c("link_id", "startdt", "endgracedt"),
                    type = "any", mult = "first", nomatch=NA) |> #not first bc bup/mth pre ntx
    mutate(fudt_discona_alt = endgracedt,
           fudays_discona_alt = fudt_discona_alt - mouddate) |>
    select(link_id,mouddate,moud,starts_with("fu"))
  
  print(paste("missing in a_alt: ",nrow(any_alt |> filter(is.na(fudt_discona_alt)))))
  
  # Combine
  discon <- any |>
    left_join(any_alt,by=c("link_id","mouddate","moud")) |>
    left_join(specific,by=c("link_id","mouddate","moud")) |>
    left_join(bups,by=c("link_id","mouddate","moud")) |>
    mutate(fudt_disconb = fifelse(moud=="bup",fudt_disconb,fudt_disconc),
           fudays_disconb = fudt_disconb - mouddate)
  
  return(discon)
}

fu_discon <- rbind(getdts(1),
              getdts(2),
              getdts(3),
              getdts(4),
              getdts(5),
              getdts(6)) |> distinct()

nrow(fu_discon |> select(link_id,mouddate,moud) |> distinct())


#################
# Save output
#################

saveRDS(fu_discon,paste0(outpath,"fu_trt.rds"))
print(paste0(nrow(fu_discon),"in fu_trt"))
toc()
