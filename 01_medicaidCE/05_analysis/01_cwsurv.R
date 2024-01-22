############################################
############################################
# Crude survival analysis
# Author: Rachael Ross
############################################
############################################

#################
# Libraries
#################

library(tidyverse)
library(tictoc)
library(data.table)
library(ggplot2)
library(rms)
library(survival)

#################
# Paths/load data
#################

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
dat <- readRDS(paste0(clean,"analysisdat_flat",cohortnum,".rds"))

### Load covariate list
source("/home/rr3551/moudr01/scripts/01_medicaidCE/05_analysis/00_covarlist.R")

#################
# Crude KM analysis
#################

ckmfun <- function(out, data, interval){
  time <- sym(paste0("event",out,"_",interval))
  delta <- sym(paste0("event",out,"_delta"))
  
  km <- survfit(Surv(dat[[time]], dat[[delta]]) ~ moud, data = data, conf.type = "log-log"
                )
  # Dataset of risks
  risks <- data.frame(t = summary(km)$time,
                       s = summary(km)$surv,
                       s_lcl = summary(km)$lower,
                       s_ucl = summary(km)$upper,
                       se = summary(km)$std.err,
                       ntx = summary(km)$strata) |>
    rbind(tibble(t=rep(if(interval=="days"){14}else{2},2),
                   s = rep(1,2),
                   s_lcl = rep(1,2),
                   s_ucl = rep(1,2),
                   se = rep(0,2),
                   ntx = unique(summary(km)$strata))) |>
    complete(t,ntx) |>
    arrange(ntx,t) |> 
    group_by(ntx) |>
    fill(everything())  |>
    mutate(r = 1-s,
           r_lcl = 1 -  s_ucl,
           r_ucl = 1 - s_lcl,
           out = out) |>
    ungroup()
  levels(risks$ntx) <- c("O-BUP","XR-NTX")
  
  return(risks)
}

outcomes <- c("1a","1b","1c","2")
crisk_wks <-  reduce(map(outcomes,ckmfun, data = dat, interval="wks"), 
                    ~rbind(.x,.y))
crisk_days <-  reduce(map(outcomes,ckmfun, data = dat, interval="days"), 
                     ~rbind(.x,.y))

saveRDS(crisk_wks,paste0(outpath,"crisk_wks","_cohort",cohortnum,".rds"))
saveRDS(crisk_days,paste0(outpath,"crisk_days","_cohort",cohortnum,".rds"))



#################
# Weighted KM analysis
#################

# Replace missing vars with 0
dat_ <- dat |>
  mutate(dem_re_othunk = case_when(is.na(dem_re)~1, # create re variable that is other/missing
                                   dem_re_ainh==1|dem_re_pinh==1|dem_re_anh==1~1,
                                   .default=0),
         across(starts_with("dem_re_"), ~replace_na(.x, 0)), # replace NAs with 0 in other re vars
         ntx = case_when(moud=="ntx"~1,.default=0)) # create trt variable


# Covariates - replace continuous with splines
covars[1] <- "rcs(dem_age)"
covars[33] <- "rcs(util_oscntex14)"

# Propensity score model
psmodel <- glm(reformulate(covars, response = "ntx"), 
               data = dat_,
               family = 'binomial')
# summary(psmodel)

# Obtain pscore and weights
dat_ <- dat_ |> 
  mutate(pscore = predict(psmodel, dat_, type = "response"),
         iptw_stab = ntx*mean(ntx)/pscore + (1-ntx)*(1-ntx)/(1-pscore),
         iptw_unstab = ntx/pscore + (1-ntx)/(1-pscore))


# Examine distribution of pscore and weights
# summary(dat_$pscore)
# by(dat_$pscore, dat_$ntx, summary)
# ggplot(dat_, aes(x=pscore, group=ntx)) +
#   geom_density()
# summary(dat_$iptw_unstab)
# by(dat_$iptw_unstab, dat_$ntx, summary)
# summary(dat_$iptw_stab)
# by(dat_$iptw_stab, dat_$ntx, summary)
# 
# justpscore <- dat_ |>
#   select(ntx,pscore,iptw_unstab,iptw_stab)
# names(justpscore)
# saveRDS(justpscore,paste0(outpath,"pscore","_cohort",cohortnum,".rds"))


# Weighted KM analysis
wkmfun <- function(out, data, interval){
  time <- sym(paste0("event",out,"_",interval))
  delta <- sym(paste0("event",out,"_delta"))
  
  km <- survfit(Surv(event1a_wks, event1a_delta) ~ moud, data = dat_, weights = iptw_stab, robust=TRUE, conf.type = "log-log")
  km <- survfit(Surv(dat[[time]], dat[[delta]]) ~ moud, data = data, weights = iptw_stab, robust=TRUE, conf.type = "log-log")
  # Dataset of risks
  risks <- data.frame(t = summary(km)$time,
                      s = summary(km)$surv,
                      s_robustlcl = summary(km)$lower,
                      s_robustucl = summary(km)$upper,
                      robustse = summary(km)$std.err,
                      ntx = summary(km)$strata) |>
    rbind(tibble(t=rep(if(interval=="days"){14}else{2},2),
                 s = rep(1,2),
                 s_robustlcl = rep(1,2),
                 s_robustucl = rep(1,2),
                 robustse = rep(0,2),
                 ntx = unique(summary(km)$strata))) |>
    complete(t,ntx) |>
    arrange(ntx,t) |> 
    group_by(ntx) |>
    fill(everything()) |>
    mutate(r = 1-s,
           r_robustlcl = 1 -s_robustucl,
           r_robustucl = 1 - s_robustlcl,
           out=out) |>
    ungroup()
  levels(risks$ntx) <- c("O-BUP","XR-NTX")
  return(risks)
}

outcomes <- c("1a","1b","1c","2")
wrisk_wks <-  reduce(map(outcomes, wkmfun, data=dat_, interval="wks"), 
                     ~rbind(.x,.y))
wrisk_days <- reduce(map(outcomes, wkmfun, data=dat_, interval="days"), 
                     ~rbind(.x,.y))


saveRDS(wrisk_wks,paste0(outpath,"wrisk_wks","_cohort",cohortnum,".rds"))
saveRDS(wrisk_days,paste0(outpath,"wrisk_days","_cohort",cohortnum,".rds"))

# #### For bootstrap
# 
# # Function to bs pscore model
# psboot <- function(nreps,data){
#   tibble(rep = 1:nreps, 
#          sample = replicate(nreps, data[sample(1:nrow(data), nrow(data), replace=TRUE), ], simplify = FALSE),
#          psmodel = map(sample, ~glm(reformulate(covars, response = "ntx"),
#                                     data=., 
#                                     family=binomial)),
#          df_wgts = map2(sample, psmodel, 
#                         ~.x %>% 
#                           mutate(pscore = predict(.y, newdata=., type='response'),
#                                  iptw_stab = ntx*mean(ntx)/pscore + (1-ntx)*(1-ntx)/(1-pscore)))) |>
#     dplyr::select(rep,df_wgts)
# }
# 
# # Function for lagging to fill in missing survival times
# mylag <- function(var){
#   ifelse(is.na(var), lag(var), var)
# }
# 
# # Function to do KM analysis with bs pscore model
# kmboot <- function(out,interval,psboot_df){
#   time <- sym(paste0("event",out,"_",interval))
#   delta <- sym(paste0("event",out,"_delta"))
#   
#   if(interval=="days"){
#     tmin <- 15
#     tmax <- 180
#   }else{
#     tmin <- 3
#     tmax <- 26
#   }
#   
#   # Create empty frame of times
#   emptytframe <- rbind(tibble(rep = 1:nreps,
#                               ntx="1") |>
#                          uncount(tmax) |>
#                          mutate(t=row_number(),.by=rep),
#                        tibble(rep = 1:nreps,
#                               ntx="0") |>
#                          uncount(tmax) |>
#                          mutate(t=row_number(),.by=rep)) |>
#     filter(t>=tmin)
#   
#   # KM analysis
#   kmsurv <- df_boot |>
#     mutate(km = map(df_wgts, ~survfit(Surv(.[[sym(time)]], .[[sym(delta)]]) ~ moud, data=., weights=iptw_stab)),
#            risks = map(km, ~tibble(t = .$time,
#                                    s = .$surv,
#                                    ntx = as.factor(c(rep(0, .$strata["moud=bup"]), rep(1, .$strata["moud=ntx"])))))) |>
#     dplyr::select(rep,risks)
#   
#   # Processing output
#   unnest(kmsurv,risks) |> 
#     full_join(emptytframe, by=c("t","rep","ntx")) |>
#     arrange(rep,ntx,t) |>
#     mutate(s=mylag(mylag(mylag(mylag(mylag(mylag(mylag(s))))))),.by=c(rep,ntx)) |>
#     group_by(t,ntx) |>
#     summarise(bsse=sd(s)) |>
#     ungroup() |>
#     mutate(out=out)
# }
# 
# ## Run boostrap
# psboot_df <- psboot(1000,dat_)
# 
# outcomes <- c("1a","1b","1c","2")
# bsse_wks <-  reduce(map(outcomes, kmboot, interval="wks", psboot_df), 
#                      ~rbind(.x,.y))
# bsse_days <- reduce(map(outcomes, kmboot, interval="days", psboot_df), 
#                     ~rbind(.x,.y))
# 
# ## Merge with point estimates from above
# wrisk_d <- wrisk_days |>
#   inner_join(bsse_days, by=c("t","ntx","out")) |>
#   mutate(r_lcl = r - 1.96*bsse,
#          r_ucl= r + 1.96*bsse)
# wrisk_w <- wrisk_wks |>
#   inner_join(bsse_wks, by=c("t","ntx","out")) |>
#   mutate(r_lcl = r - 1.96*bsse,
#          r_ucl= r + 1.96*bsse)
# 
# saveRDS(wrisk_w,paste0(outpath,"wrisk_wks","_cohort",cohortnum,".rds"))
# saveRDS(wrisk_d,paste0(outpath,"wrisk_days","_cohort",cohortnum,".rds"))
#                  
#  
# 
# 




