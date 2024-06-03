############################################-
############################################-
# Survival analysis of outcomes
# Author: Rachael Ross
############################################-
############################################-

#################-
# Libraries----
#################-

library(tidyverse)
library(purrr)
library(isotone)

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

### Load data
cohortnum <- 2
dat <- readRDS(paste0(clean,"analysisdat_wide",cohortnum,".rds"))
## CHECK THAT DATA IS WEEKS names(dat)

### Load covariate list
source("/home/rr3551/moudr01/scripts/01_medicaidCE/05_analysis/00_covarlist.R")

### Get outcome variable
outcome <- commandArgs(trailingOnly=TRUE)

#################-
# Process data----
#################-

# Remove missing in race/ethnicity and define binary trt
dat <- dat |>
  mutate(dem_re_othunk = case_when(is.na(dem_re)~1, # create re variable that is other/missing
                                   dem_re_ainh==1|dem_re_pinh==1|dem_re_anh==1~1,
                                   .default=0),
         across(starts_with("dem_re_"), ~replace_na(.x, 0)), # replace NAs with 0 in other re vars
         ntx = case_when(moud=="ntx"~1,.default=0),
         nj = ifelse(STATE=="NJ",1,0)) |>
  as.data.frame()

#################-
# LMTP - survival outcome ----
#################-

# Define treatment and covariate vectors
A <- "ntx"
#W <- c("dem_male")
W <- covars

# Define library
#lib <- c("mean","glm")
lib <- c("mean","glm","earth","xgboost")

#### Functions ----

# Extract diagnostics
extractdiag <- function(object,trt){
  tibble(eif = object$eif,
         outcome_reg = object$outcome_reg,
         density_ratios = object$density_ratios,
         trt = trt)
}

# Extract results
extractres <- function(object){
  list(theta = object$theta,
       eif = object$eif)
}

# # Just a single time point (survival)
# Y <- c(paste0("out",out,"_Y_",2:10))
# C <- c(paste0("out",out,"_C_",1:9))
# y_1 <- lmtp_sdr(dat, A, Y, W, cens = C,
#                    outcome_type = "survival", 
#                    folds = 1,
#                    learners_trt = lib,
#                    learners_outcome = lib,
#                    shift = static_binary_on)
# y_0 <- lmtp_sdr(dat, A, Y, W, cens = C,
#                    outcome_type = "survival", 
#                    folds = 1,
#                    learners_trt = lib,
#                    learners_outcome = lib,
#                    shift = static_binary_off)
# lmtp_contrast(y_1, ref = y_0, type = "additive")


# Run survival lmtp_sdr
survlmtp <- function(Y,C,data,cffolds,diag=FALSE,intervention){
  out %<-% lmtp_sdr(data, A, Y, W, cens = C,
                   outcome_type = "survival", 
                   folds = cffolds,
                   learners_trt = lib,
                   learners_outcome = lib,
                   shift = intervention)

  # results <- list("ntx"=extractres(y_1),
  #                 "bup"=extractres(y_0))

  print(paste0("One time point done ",Y[length(Y)]))

  # if(diag==TRUE){
  #   diag <- rbind(extractdiag(y_1,"ntx"),
  #                 extractdiag(y_0,"bup"))
  #   return(list("results"=results,"diag"=diag))
  # }else{
  #   return(results)
  # }
  return(out[c("theta","eif","standard_error")])
}

# Y <- c(paste0("out",outcome,"_Y_",2:23))
# C <- c(paste0("out",outcome,"_C_",1:22))
# set.seed(7)
# test <- survlmtp(Y,C,dat,1,FALSE,static_binary_off)


# Create list of time points with Y and C variables for a single outcome
createlists <- function(out,Ymin,Ymax){
  len <- Ymax - Ymin + 1
  
  Y <- vector("list",len)
  C <- vector("list",len)
  
  for (i in 1:len){
    Y[[i]] <- c(paste0("out",out,"_Y_",2:(Ymin + (i-1))))
    C[[i]] <- c(paste0("out",out,"_C_",1:(Ymin + (i-2))))
  }
  
  list(Y,C)
}



# #### Run multiple time pts for each outcome ----
set.seed(73)
plan(multicore)
tic()

points <- createlists(outcome,if(outcome=="2"){3}else{6},26)
#points <- createlists(outcome,if(outcome=="2"){3}else{6},10)
surv1 <- future_map2(points[[1]],points[[2]],survlmtp,dat,1,FALSE,static_binary_on)
surv0 <- future_map2(points[[1]],points[[2]],survlmtp,dat,1,FALSE,static_binary_off)

saveRDS(list(trted = surv1,
             cntrl = surv0),
        paste0(outpath,"lmtpall",outcome,"_cohort",cohortnum,"_raw.rds"))
toc()


#### Run last time point to assess diagnostics ----
#set.seed(73)

#plan(multicore)
#progressr::handlers(global = TRUE)

# outcome <- "1a"
# tic()
# lastsurv <- survlmtp(paste0("out",outcome,"_Y_",3:26),
#                      paste0("out",outcome,"_C_",2:25),
#                      dat,
#                      cffolds = 1,
#                      diag = TRUE)
# #summary(lastsurv$diag$eif)
# #eif <- lastsurv$diag$eif
# saveRDS(lastsurv,paste0(outpath,"lmtp",outcome,"_cohort",cohortnum,".rds"))
# toc()


