############################################-
############################################-
# Process survival analysis results
# Author: Rachael Ross
############################################-
############################################-

#################-
# Libraries----
#################-

library(tidyverse)
library(purrr)
library(isotone)

library(tictoc)
library(data.table)
library(future)
library(furrr)
library(progressr)

#################-
# Paths/load data----
#################-

# Output
outpath <- "/home/rr3551/moudr01/output/01_medicaidCE/"

### Load utility functions
source("/home/rr3551/moudr01/scripts/01_medicaidCE/05_analysis/utils.R")

### Load data
#survdat <- readRDS(paste0(outpath,"lmtpall1a_cohort2_raw.rds"))

process_raw <- function(data){
  # Raw results
  surv_summary <- lapply(list(data$trted, data$cntrl), function(x) {
      surv_est <- do.call(c, lapply(x, `[[`, "theta"))
      surv_se <- do.call(c, lapply(x, `[[`, "standard_error"))
      surv_results <- as_tibble(
        list(
          time = seq_along(x),
          est = 1-surv_est,
          std_err = surv_se,
          ci_lwr = (1-surv_est) - abs(qnorm(p = (1 - .95) / 2))*surv_se,
          ci_upr = (1-surv_est) + abs(qnorm(p = (1 - .95) / 2))*surv_se
        ))
      
      return(surv_results)
    }) %>% set_names(c("trt", "ctl"))
  
  surv_summary$rd <- tibble(
    time = seq_along(surv_summary$trt$est),
    est = surv_summary$trt$est - surv_summary$ctl$est,
    std_err = sqrt(surv_summary$trt$std_err^2 + surv_summary$ctl$std_err^2),
    ci_lwr = est - abs(qnorm(p = (1 - .95) / 2))*std_err,
    ci_upr = est + abs(qnorm(p = (1 - .95) / 2))*std_err)
    
  
  eif_summary <- lapply(list(data$trted, data$cntrl), function(x) {
      eif_mat <- 1 - do.call(cbind, lapply(x, `[[`, "eif"))
      return(eif_mat)
    }) %>% set_names(c("trt", "ctl"))
  
  # Use isotonic regression to enforce monotonicitiy
  ## Simultaneous CIs
  surv_summary$trt_isosim <- isoproj(surv_summary$trt, eif_summary$trt,
                                0.95, "simult") 
  surv_summary$ctl_isosim <- isoproj(surv_summary$ctl, eif_summary$ctl,
                                0.95, "simult")
  
  surv_summary$rd_isosim <- tibble(
    time = seq_along(surv_summary$trt$est),
    est = surv_summary$trt_isosim$est - surv_summary$ctl_isosim$est,
    std_err = sqrt(surv_summary$trt_isosim$std_err^2 + surv_summary$ctl_isosim$std_err^2),
    ci_lwr = est - cb_simult(eif_summary$trt - eif_summary$ctl, 0.95)*std_err,
    ci_upr = est + cb_simult(eif_summary$trt - eif_summary$ctl, 0.95)*std_err)
  
  ## Marginal CIs
  surv_summary$trt_isomarg <- isoproj(surv_summary$trt, eif_summary$trt,
                                0.95, "marginal") 
  surv_summary$ctl_isomarg <- isoproj(surv_summary$ctl, eif_summary$ctl,
                                0.95, "marginal")
  
  surv_summary$rd_isomarg <- tibble(
    time = seq_along(surv_summary$trt$est),
    est = surv_summary$trt_isomarg$est - surv_summary$ctl_isomarg$est,
    std_err = sqrt(surv_summary$trt_isomarg$std_err^2 + surv_summary$ctl_isomarg$std_err^2),
    ci_lwr = est - abs(qnorm(p = (1 - .95) / 2))*std_err,
    ci_upr = est + abs(qnorm(p = (1 - .95) / 2))*std_err)
  
  return(surv_summary)
}

tooutput <- function(results,which){
  stacked <- rbind(results[[paste0("trt",which)]] |> mutate(trt="ntx"),
                   results[[paste0("ctl",which)]] |> mutate(trt="bup"),
                   results[[paste0("rd",which)]] |> mutate(trt="rd")) |>
    rename(se = std_err,
           lcl = ci_lwr,
           ucl = ci_upr) |>
    #arrange(time,trt) |>
    mutate(t = (26 - max(time)) + time) |>
    select(-time)
  
  return(stacked)                 
}

out1a <- process_raw(readRDS(paste0(outpath,"lmtpall1a_cohort2_raw.rds")))
# rd <- out1a$rd |> mutate(cil = ci_upr - ci_lwr)
# rd_isomarg <- out1a$rd_isomarg |> mutate(cil = ci_upr - ci_lwr)#made no difference
# rd_isosim <- out1a$rd_isosim |> mutate(cil = ci_upr - ci_lwr) #difference in CIs - about 1-2 percent points
# round(rd_isosim - rd,digits = 5)

saveRDS(tooutput(out1a,""),paste0(outpath,"lmtpall1a_cohort2.rds"))

out1c <- process_raw(readRDS(paste0(outpath,"lmtpall1c_cohort2_raw.rds")))
# rd <- out1c$rd |> mutate(cil = ci_upr - ci_lwr)
# rd_isomarg <- out1c$rd_isomarg |> mutate(cil = ci_upr - ci_lwr)#made no difference
# rd_isosim <- out1c$rd_isosim |> mutate(cil = ci_upr - ci_lwr) #difference in CIs - about 1-2 percent points
# round(rd_isosim - rd,digits = 5)

saveRDS(tooutput(out1c,""),paste0(outpath,"lmtpall1c_cohort2.rds"))

out2 <- process_raw(readRDS(paste0(outpath,"lmtpall2_cohort2_raw.rds")))
# rd <- out2$rd |> mutate(cil = ci_upr - ci_lwr)
# rd_isomarg <- out2$rd_isomarg |> mutate(cil = ci_upr - ci_lwr)#made no difference
# rd_isosim <- out2$rd_isosim |> mutate(cil = ci_upr - ci_lwr) #difference in CIs - about 1-2 percent points
# round(rd_isosim - rd,digits = 5)

saveRDS(tooutput(out2,"_isomarg"),paste0(outpath,"lmtpall2_cohort2.rds"))

