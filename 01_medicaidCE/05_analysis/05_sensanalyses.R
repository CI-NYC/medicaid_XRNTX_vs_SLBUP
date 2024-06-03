############################################-
############################################-
# Sensitivity analyses
# Author: Rachael Ross
############################################-
############################################-

#################-
# Libraries----
#################-

library(tidyverse)
library(purrr)

library(lmtp)
library(mlr3superlearner)
library(mlr3learners)
library(mlr3)
library(mlr3extralearners)

library(tictoc)
library(data.table)
library(future)
library(furrr)
library(progressr)

library(tmle)
library(PSweight)

#################-
# Paths/load data----
#################-

### Paths

projpath <- "/home/data/moud/01_medicaidCE/"

# Full
clean <- paste0(projpath,"clean/")

# # Sample
# clean <- paste0(projpath,"sample/clean/")

# Output
outpath <- "/home/rr3551/moudr01/output/01_medicaidCE/"

### Load covariate list
source("/home/rr3551/moudr01/scripts/01_medicaidCE/05_analysis/00_covarlist.R")

# See which se analysis to do
analysisnum <- commandArgs(trailingOnly=TRUE)

set.seed(37)
plan(multicore)

#################-
# Process data----
#################-

tic()


### Load data
if(analysisnum<8){
  cohortnum <- 2
}else if (analysisnum<10){
  cohortnum <- 3
}else{
  cohortnum <- 7
}

dat <- readRDS(paste0(clean,"analysisdat_flat",cohortnum,".rds"))

# Remove missing in race/ethnicity and define binary trt
dat <- dat |>
  mutate(dem_re_othunk = case_when(is.na(dem_re)~1, # create re variable that is other/missing
                                   dem_re_ainh==1|dem_re_pinh==1|dem_re_anh==1~1,
                                   .default=0),
         across(starts_with("dem_re_"), ~replace_na(.x, 0)), # replace NAs with 0 in other re vars
         ntx = case_when(moud=="ntx"~1,.default=0),
         nj = ifelse(STATE=="NJ",1,0)) |>
  as.data.frame()



#lib <- c("SL.mean","SL.glm")
lib <- c("SL.mean","SL.glm","SL.earth","SL.xgboost")


#################-
# TREATMENT DISCONTINUATION OUTCOME ----
#################-

## ATT ----

if(analysisnum==1) {

  A <- dat$ntx
  W <- dat[,covars]
  #W <- dat[,c("dem_male","psy_depression")]
  #lib <- c("SL.mean","SL.glm")
  
  Y <- dat$event1a_Y180
  C <- dat$event1a_C180
  
  Y_nomiss <- ifelse(is.na(Y),0,Y)
  
  att %<-% tmle(Y_nomiss,A,W,Delta=C,
              Q.SL.library = lib,
              g.SL.library = lib,
              g.Delta.SL.library = lib,
              automate = TRUE, family= "binomial")
  
  saveRDS(att,paste0(outpath,"se1a","_att.rds"))
  #readRDS(paste0(outpath,"tmle1a","_cohort",cohortnum,".rds"))
}


## Optimally trimmed ----

if(analysisnum==2) {

  
  A <- "ntx"
  W <- covars
  
  # # Fit pscore model
  # psmod <- mlr3superlearner(
  #   data = dat[,c(A,W)],
  #   target = A,
  #   library = c("glm"),
  #   outcome_type = "binomial"
  # )
  
  psmod <- SuperLearner(Y = dat[,A], X = dat[,W], 
                        SL.library = lib,  family = binomial())

  # Get predictions
  pscore <- dat %>%
    #mutate(pscore = predict(psmod, newdata=.,type="response"))
    mutate(pscore =  psmod$SL.predict)
    #mutate(pscore = predict.SuperLearner(psmod, newdata=.)$pred) 
  
  # summary(pscore$pscore[pscore$ntx==1])
  # summary(pscore$pscore[pscore$ntx==0])
  # ggplot(pscore, aes(x=pscore, group=ntx, color=ntx)) + geom_density() + theme_bw()
  
  #print("Numbers trimmed")
  totrim <- PStrim(pscore,zname="ntx",ps.estimate = pscore$pscore,optimal=TRUE)
  #print(totrim)
  #print(totrim$delta)
  
  
  trimmed <- totrim$data
  #table(trimmed$ntx)
  
  # TMLE with trimmed cohort
  A <- trimmed$ntx
  W <- trimmed[,covars]
  Y <- trimmed$event1a_Y180
  C <- trimmed$event1a_C180
  Y_nomiss <- ifelse(is.na(Y),0,Y)

  trim %<-% tmle(Y_nomiss,A,W,Delta=C,
               Q.SL.library = lib,
               g.SL.library = lib,
               g.Delta.SL.library = lib,
               automate = TRUE, family= "binomial")
  
  # print("Trimmed analysis results")
  # print(trim)
  
  saveRDS(totrim$delta,paste0(outpath,"se1a","_trim_n.rds"))
  saveRDS(trim,paste0(outpath,"se1a","_trim.rds"))
}


## Grace period sensitivity analysis ----

if(analysisnum==3) {
  A <- dat$ntx
  W <- dat[,covars]

  Y <- dat$eventseg1a_Y180
  C <- dat$eventseg1a_C180
  
  Y_nomiss <- ifelse(is.na(Y),0,Y)
  
  segrace %<-% tmle(Y_nomiss,A,W,Delta=C,
                  Q.SL.library = lib,
                  g.SL.library = lib,
                  g.Delta.SL.library = lib,
                  automate = TRUE, family= "binomial")
  
  
  # print("Shorter grace period")
  # print(segrace)
  saveRDS(segrace,paste0(outpath,"se1a","_grace.rds"))
}



## LTFU sensitivity analysis ----

if(analysisnum==4) {
  A <- dat$ntx
  W <- dat[,covars]

  Y <- dat$eventse1a_Y180
  C <- dat$eventse1a_C180
  
  Y_nomiss <- ifelse(is.na(Y),0,Y)
  
  seltfu %<-% tmle(Y_nomiss,A,W,Delta=C,
                 Q.SL.library = lib,
                 g.SL.library = lib,
                 g.Delta.SL.library = lib,
                 automate = TRUE, family= "binomial")
  
  # print("Include LTFU as outcome")
  # print(seltfu)
  saveRDS(seltfu,paste0(outpath,"se1a","_ltfu.rds"))
}



## 3-months look back - ATE ----

if(analysisnum==8) {

  A <- dat$ntx
  W <- dat[,covars]
  W <- W[,names(W)!="com_chronicpn"] # remove bc not defined in 90 days

  Y <- dat$event1a_Y180
  C <- dat$event1a_C180
  
  Y_nomiss <- ifelse(is.na(Y),0,Y)
  
  att %<-% tmle(Y_nomiss,A,W,Delta=C,
              Q.SL.library = lib,
              g.SL.library = lib,
              g.Delta.SL.library = lib,
              automate = TRUE, family= "binomial")
  
  saveRDS(att,paste0(outpath,"se1a","_3molb.rds"))
  #readRDS(paste0(outpath,"tmle1a","_cohort",cohortnum,".rds"))
}


## >7 days bup at base - ATE ----

if(analysisnum==10) {

  A <- dat$ntx
  W <- dat[,covars]

  Y <- dat$event1a_Y180
  C <- dat$event1a_C180
  
  Y_nomiss <- ifelse(is.na(Y),0,Y)
  
  att %<-% tmle(Y_nomiss,A,W,Delta=C,
              Q.SL.library = lib,
              g.SL.library = lib,
              g.Delta.SL.library = lib,
              automate = TRUE, family= "binomial")
  
  saveRDS(att,paste0(outpath,"se1a","_gr7bup.rds"))
}


#################-
# OVERDOSE OUTCOME ----
#################-

## ATT ----

if(analysisnum==5) {
  A <- dat$ntx
  W <- dat[,covars]

  Y <- dat$event2_Y180
  C <- dat$event2_C180
  
  Y_nomiss <- ifelse(is.na(Y),0,Y)
  
  att %<-% tmle(Y_nomiss,A,W,Delta=C,
              Q.SL.library = lib,
              g.SL.library = lib,
              g.Delta.SL.library = lib,
              automate = TRUE, family= "binomial")
  
  saveRDS(att,paste0(outpath,"se2","_att.rds"))
}



## Optimally trimmed ----

if(analysisnum==6) {
  A <- "ntx"
  W <- covars

  # # Fit pscore model
  # psmod <- mlr3superlearner(
  #   data = dat[,c(A,W)],
  #   target = A,
  #   library = c("glm"),
  #   outcome_type = "binomial"
  # )
  
  psmod <- SuperLearner(Y = dat[,A], X = dat[,W], 
                        SL.library = lib,  family = binomial())

  # Get predictions
  pscore <- dat %>%
    #mutate(pscore = predict(psmod, newdata=.,type="response"))
    mutate(pscore =  psmod$SL.predict)
  
  # summary(pscore$pscore[pscore$ntx==1])
  # summary(pscore$pscore[pscore$ntx==0])
  # ggplot(pscore, aes(x=pscore, group=ntx, color=ntx)) + geom_density() + theme_bw()
  
  #print("Numbers trimmed")
  totrim <- PStrim(pscore,zname="ntx",ps.estimate = pscore$pscore,optimal=TRUE)
  #print(totrim)
  #print(totrim$delta)
  
  trimmed <- totrim$data
  #table(trimmed$ntx)
  
  # TMLE with trimmed cohort
  A <- trimmed$ntx
  W <- trimmed[,covars]
  Y <- trimmed$event2_Y180
  C <- trimmed$event2_C180
  Y_nomiss <- ifelse(is.na(Y),0,Y)

  trim %<-% tmle(Y_nomiss,A,W,Delta=C,
               Q.SL.library = lib,
               g.SL.library = lib,
               g.Delta.SL.library = lib,
               automate = TRUE, family= "binomial")
  
  # print("Trimmed analysis results")
  # print(trim)
  
  saveRDS(trimmed$ntx,paste0(outpath,"se2","_trim_n.rds"))
  saveRDS(trim,paste0(outpath,"se2","_trim.rds"))
}



## More inclusive definition ----

if(analysisnum==7) {
  # Remove folks with early outcome
  datnew <- dat |>
    filter(excl_earlyoverdosealt==0)
  
  #print("more inclusive overdose definition")
  #print(nrow(datnew$ntx))
  
  A <- datnew$ntx
  W <- datnew[,covars]
  #W <- dat[,c("dem_male","psy_depression")]
  #lib <- c("SL.mean","SL.glm")

  Y <- datnew$eventse2_Y180
  C <- datnew$eventse2_C180
  
  Y_nomiss <- ifelse(is.na(Y),0,Y)
  
  overdosealt %<-% tmle(Y_nomiss,A,W,Delta=C,
                      Q.SL.library = lib,
                      g.SL.library = lib,
                      g.Delta.SL.library = lib,
                      automate = TRUE, family= "binomial")
  
  #print(overdosealt)
  saveRDS(datnew$ntx,paste0(outpath,"se2","_altdef_n.rds"))
  saveRDS(overdosealt,paste0(outpath,"se2","_altdef.rds"))
}

## 3-months look back - ATE ----

if(analysisnum==9) {

  A <- dat$ntx
  W <- dat[,covars]
  W <- W[,names(W)!="com_chronicpn"] # remove bc not defined in 90 days

  Y <- dat$event2_Y180
  C <- dat$event2_C180
  
  Y_nomiss <- ifelse(is.na(Y),0,Y)
  
  att %<-% tmle(Y_nomiss,A,W,Delta=C,
              Q.SL.library = lib,
              g.SL.library = lib,
              g.Delta.SL.library = lib,
              automate = TRUE, family= "binomial")
  
  saveRDS(att,paste0(outpath,"se2","_3molb.rds"))
}


## >7 days bup at base - ATE ----

if(analysisnum==11) {

  A <- dat$ntx
  W <- dat[,covars]

  Y <- dat$event2_Y180
  C <- dat$event2_C180
  
  Y_nomiss <- ifelse(is.na(Y),0,Y)
  
  att %<-% tmle(Y_nomiss,A,W,Delta=C,
              Q.SL.library = lib,
              g.SL.library = lib,
              g.Delta.SL.library = lib,
              automate = TRUE, family= "binomial")
  
  saveRDS(att,paste0(outpath,"se2","_gr7bup.rds"))
}


toc()

###############################-
# Results ----
###############################-

# Stratified analyses
readRDS(paste0(outpath,"lmtp1a_cohort2com_chronicpn.rds"))
readRDS(paste0(outpath,"lmtp1a_cohort2sud_alcohol.rds"))

# Se analysis
# Discontinuation outcome
readRDS(paste0(outpath,"se1a_att.rds"))$estimates$ATE
readRDS(paste0(outpath,"se1a_att.rds"))$estimates$ATT
readRDS(paste0(outpath,"se1a_trim.rds"))$estimates$ATE
readRDS(paste0(outpath,"se1a_trim_n.rds"))
readRDS(paste0(outpath,"se1a_ltfu.rds"))$estimates$ATE
readRDS(paste0(outpath,"se1a_grace.rds"))$estimates$ATE
readRDS(paste0(outpath,"se1a_3molb.rds"))$estimates$ATE
table(readRDS(paste0(clean,"analysisdat_flat3.rds"))[,"moud"])
readRDS(paste0(outpath,"se1a_gr7bup.rds"))$estimates$ATE
table(readRDS(paste0(clean,"analysisdat_flat7.rds"))[,"moud"])

# Overdose
readRDS(paste0(outpath,"se2_att.rds"))$estimates$ATE
readRDS(paste0(outpath,"se2_att.rds"))$estimates$ATT
readRDS(paste0(outpath,"se2_trim.rds"))$estimates$ATE
table(readRDS(paste0(outpath,"se2_trim_n.rds")))
readRDS(paste0(outpath,"se2_altdef.rds"))$estimates$ATE
table(readRDS(paste0(outpath,"se2_altdef_n.rds")))
readRDS(paste0(outpath,"se2_3molb.rds"))$estimates$ATE
readRDS(paste0(outpath,"se2_gr7bup.rds"))$estimates$ATE

