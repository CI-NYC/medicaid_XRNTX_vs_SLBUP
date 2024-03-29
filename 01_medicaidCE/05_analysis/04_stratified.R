############################################-
############################################-
# Survival analysis of outcomes - stratified
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

# Get command line info
args <- commandArgs(trailingOnly=TRUE)
outcome <- args[[1]]
stratvar <- args[[2]]

#outcome <- "1a"
#stratvar <- "com_chronicpn"


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

dat1 <- dat |>
  filter(!!ensym(stratvar)==1)

dat0 <- dat |>
  filter(!!ensym(stratvar)==0)

#################-
# LMTP - survival outcome ----
#################-

# Define treatment and covariate vectors
A <- "ntx"
#W <- c("dem_male")
W <- covars[covars!=stratvar]

# Define library
#lib <- c("mean","glm")
lib <- c("mean","glm","lightgbm","earth")

#### Functions ----

# Extract diagnostics
extractdiag <- function(object,trt){
  tibble(eif = object$eif,
         outcome_reg = object$outcome_reg,
         density_ratios = object$density_ratios,
         trt = trt)
}

# Extract results
extractres <- function(object,trt){
  tibble(est = 1-object$theta,
         se = object$standard_error,
         lcl = 1-object$high,
         ucl = 1-object$low,
         trt = trt)
}

# Run survival lmtp_sdr
survlmtp <- function(Y,C,data,cffolds,diag=FALSE){
  y_1 %<-% lmtp_sdr(data, A, Y, W, cens = C,
                   outcome_type = "survival", 
                   folds = cffolds,
                   learners_trt = lib,
                   learners_outcome = lib,
                   shift = static_binary_on)
  y_0 %<-% lmtp_sdr(data, A, Y, W, cens = C,
                   outcome_type = "survival", 
                   folds = cffolds,
                   learners_trt = lib,
                   learners_outcome = lib,
                   shift = static_binary_off)
  sd %<-% lmtp_contrast(y_1, ref = y_0, type = "additive")
  
  results <- rbind(extractres(y_1,"ntx"),
                   extractres(y_0,"bup"),
                   tibble(est = -1*sd$vals$theta,
                          se = sd$vals$std.error,
                          lcl = -1*sd$vals$conf.high,
                          ucl = -1*sd$vals$conf.low,
                          trt = "rd"))

  print(paste0("One time point done ",Y[length(Y)]))

  if(diag==TRUE){
    diag <- rbind(extractdiag(y_1,"ntx"),
                  extractdiag(y_0,"bup"))
    return(list("results"=results,"diag"=diag))
  }else{
    return(results)
  }

}


# Create list of time points with Y and C variables for a single outcome
createlists <- function(out,Ymin,Ymax){
  len <- Ymax - Ymin + 1
  
  Y <- vector("list",len)
  C <- vector("list",len)
  
  for (i in 1:len){
    Y[[i]] <- c(paste0("out",out,"_Y_",3:(Ymax - i + 1)))
    C[[i]] <- c(paste0("out",out,"_C_",(2):(Ymax - i)))
  }
  
  list(Y,C)
}


#### Run last time point to assess diagnostics ----
#set.seed(73)

#plan(multicore)
#progressr::handlers(global = TRUE)

#outcome <- "1a"
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


# #### Run multiple time pts for each outcome ----
set.seed(75)
plan(multicore)
tic()

points <- createlists(outcome,6,26)
surv1 <- future_map2(points[[1]],points[[2]],survlmtp,dat1,1,FALSE)
surv0 <- future_map2(points[[1]],points[[2]],survlmtp,dat0,1,FALSE)

rresults1 <- reduce(surv1,rbind) |>
  mutate(t = max(row_number()) - row_number() + 6,.by=trt) |>
  mutate(strat=stratvar,level=1)
rresults0 <- reduce(surv0,rbind) |>
  mutate(t = max(row_number()) - row_number() + 6,.by=trt) |>
  mutate(strat=stratvar,level=0)

saveRDS(rbind(rresults1,rresults0),
        paste0(outpath,"lmtpall",outcome,"_cohort",cohortnum,stratvar,".rds"))
toc()

