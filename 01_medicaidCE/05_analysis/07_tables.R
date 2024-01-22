############################################
############################################
# Table of primary survival analysis
# Author: Rachael Ross
############################################
############################################

#################
# Libraries
#################
library(tidyverse)
library(ggplot2)
library(utile.visuals)
library(data.table)
library(patchwork)
library(tidyverse)
library(gtsummary)
library(gt)

#################
# Paths
#################

# Local
home <- Sys.getenv("HOME")
path <- paste0(home, "/07 Postdoc/02 Projects/02 Medicaid CE/03 Output")
setwd(path)

# # On server
# outpath <- "/home/rr3551/moudr01/output/01_medicaidCE/"
# setwd(outpath)


#################
# Load and prep data - overall results
#################

cohortnum <- 2

# Funtion for getting risk differences from crude
getrds <- function(data){
  dcast(melt(data |>
               mutate(ntxnum = ifelse(ntx=="XR-NTX",1,0)) |>
               dplyr::select(t,ntxnum,r,se,out) |> data.table(), 
             id.vars=c("t","ntxnum","out")),
        t+out~variable+ntxnum) |>
    mutate(rd = r_1 - r_0,
           se = se_1 + se_0,
           rd_lcl = rd - qnorm(.975)*se,
           rd_ucl = rd + qnorm(.975)*se)
}

# Loading data
lmtp1a <- readRDS(paste0("lmtpall1a_cohort",cohortnum,".rds")) |> mutate(out="1a")
lmtp1c <- readRDS(paste0("lmtpall1c_cohort",cohortnum,".rds"))  |> mutate(out="1c")
lmtp2 <- readRDS(paste0("lmtpall2_cohort",cohortnum,".rds"))  |> mutate(out="2")

cplmtp1a <- readRDS(paste0("lmtpall1a_cohort",cohortnum,"com_chronicpn",".rds"))
audlmtp1a <- readRDS(paste0("lmtpall1a_cohort",cohortnum,"sud_alcohol",".rds"))

#myrnd <- function(x){str_pad(round(x,1),4,pad="0",side="right")}
#myrnd2 <- function(x){str_pad(round(x,1),4,pad="0",side="right")}
myrnd <- function(x){round(x,1)}
myrnd2 <- function(x){round(x,1)}
wks <- c(9,13,18,22,26)
outs <- c("1a","1c","2")

#################-
# Table of main results ----
#################-

crude_wks <- readRDS(paste0("crisk_wks_cohort",cohortnum,".rds")) |>
  mutate(r=r*100,r_lcl=r_lcl*100,r_ucl=r_ucl*100,se=se*100) |>
  mutate(est = paste0(ifelse(out=="2",myrnd2(r),myrnd(r)),
                      " (",
                      ifelse(out=="2",myrnd2(r_lcl),myrnd(r_lcl)),
                      ", ",
                      ifelse(out=="2",myrnd2(r_ucl),myrnd(r_ucl)),
                      ")")) |>
  filter(t %in% wks & out %in% outs)

adj_wks <- rbind(lmtp1a,lmtp1c,lmtp2) |>
  filter(trt!="rd") |>
  rename(r = est, r_lcl = lcl, r_ucl = ucl) |>
  mutate(r=r*100,r_lcl=r_lcl*100,r_ucl=r_ucl*100) |>
  mutate(ntx = ifelse(trt=="bup","O-BUP","XR-NTX")) |>
  mutate(t = t + 5) |>
  dplyr::select(-trt) |>
  mutate(est = paste0(ifelse(out=="2",myrnd2(r),myrnd(r)),
                      " (",
                      ifelse(out=="2",myrnd2(r_lcl),myrnd(r_lcl)),
                      ", ",
                      ifelse(out=="2",myrnd2(r_ucl),myrnd(r_ucl)),
                      ")")) |>
  filter(t %in% wks & out %in% outs)

cruderds_wks <- getrds(crude_wks) |>
  mutate(cruderd = paste0(ifelse(out=="2",myrnd2(rd),myrnd(rd)),
                          " (",
                          ifelse(out=="2",myrnd2(rd_lcl),myrnd(rd_lcl)),
                          ", ",
                          ifelse(out=="2",myrnd2(rd_ucl),myrnd(rd_ucl)),
                          ")")) |>
  filter(t %in% wks & out %in% outs) |>
  dplyr::select(out,t,cruderd)

adjrd_wks <- rbind(lmtp1a,lmtp1c,lmtp2) |>
  filter(trt=="rd") |>
  rename(rd = est, rd_lcl = lcl, rd_ucl = ucl) |>
  mutate(rd=rd*100,rd_lcl=rd_lcl*100,rd_ucl=rd_ucl*100) |>
  mutate(t = t + 5) |>
  dplyr::select(-trt) |>
  mutate(adjrd = paste0(ifelse(out=="2",myrnd2(rd),myrnd(rd)),
                        " (",
                        ifelse(out=="2",myrnd2(rd_lcl),myrnd(rd_lcl)),
                        ", ",
                        ifelse(out=="2",myrnd2(rd_ucl),myrnd(rd_ucl)),
                        ")")) |>
  filter(t %in% wks & out %in% outs) |>
  dplyr::select(out,t,adjrd)



getlatex <- function(input){
  input |>
    as_latex() |>
    as.character() |>
    cat()
}

#Make risks wide
wide_cruder <- dcast(setDT(crude_wks |> 
                             mutate(ntxnum = ifelse(ntx=="XR-NTX","crude1","crude0")) |>
                   dplyr::select(out,t,est,ntxnum)),
      t+out~ntxnum,
      value.var=c("est")) 
  

wide_adjr <- dcast(setDT(adj_wks |> 
                           mutate(ntxnum = ifelse(ntx=="XR-NTX","adj1","adj0"))|> 
                             dplyr::select(out,t,est,ntxnum)),
                     t+out~ntxnum,
                     value.var=c("est"))

#Combine
fortab <- wide_cruder |>
  inner_join(cruderds_wks) |>
  inner_join(wide_adjr) |>
  inner_join(adjrd_wks) |>
  arrange(out,t)

fortab |>
  gt() |>
  cols_hide(columns = c(out)) |>
  tab_row_group(
    label = "Discontinuation of all MOUD or death",
    rows = out=="1a",
    id = "1a"
  ) |> 
  tab_row_group(
    label = "Discontinuation of initial medication or death",
    rows = out=="1c",
    id = "1c"
  ) |>
  tab_row_group(
    label = "Overdose or death",
    rows = out=="2",
    id = "2"
  ) |>
  row_group_order(groups = c("1a","1c","2")) |>
  cols_label(t = "Week",
             crude1 = "XR-NTX",
             adj1 = "XR-NTX",
             crude0 = "O-BUP",
             adj0 = "O-BUP",
             cruderd = "Difference",
             adjrd = "Difference") |>
  cols_align(
    align = "center",
    columns = c(starts_with("crude"),starts_with("adj"))
  ) |>
  cols_move("crude0", after="crude1") |>
  cols_move("adj0", after="adj1") |>
  gt::tab_style(
    style = gt::cell_text(style = "italic"),
    locations = gt::cells_row_groups(groups = everything())) |>
  tab_spanner(
    "Risks",
    columns = c("crude0","crude1"),
    level = 1,
    gather = FALSE,
    id = "crisks"
  )   |>
  tab_spanner(
    "Risks",
    columns = c("adj0","adj1"),
    level = 1,
    gather = FALSE,
    id = "arisks"
  )   |>
  tab_spanner(
    "Crude  (95% CI)",
    columns = starts_with("crude"),
   # spanners = c("crisks"),
    level = 2
  ) |>
  tab_spanner(
    "Adjusted  (95% CI)",
    columns = starts_with("adj"),
  #  spanners = c("arisks"),
    level = 2
  ) |>
  getlatex()



#################
# Load and prep data - stratified results
#################
options(pillar.sigfig = 7)
rbind(audlmtp1a,cplmtp1a) |>
  filter(trt=="rd") |>
  rename(rd = est, rd_lcl = lcl, rd_ucl = ucl) |>
  dplyr::select(-trt) |>
  mutate(adjrd = paste0(myrnd(rd)," (",myrnd(rd_lcl),", ",myrnd(rd_ucl),")")) |>
  filter(t %in% c(26)) 



