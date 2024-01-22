############################################-
############################################-
# Describe eif
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

library(ggplot2)

#################-
# Paths/load data----
#################-

outpath <- "/home/rr3551/moudr01/output/01_medicaidCE/"

n <- 11641

lmtp1a <- readRDS(paste0(outpath,"lmtp1a_cohort2.rds"))
lmtp1c <- readRDS(paste0(outpath,"lmtp1c_cohort2.rds"))
lmtp2 <- readRDS(paste0(outpath,"lmtp2_cohort2.rds"))

summary(lmtp1a$diag$eif[lmtp1a$diag$trt=="ntx"])
summary(lmtp1a$diag$eif[lmtp1a$diag$trt=="bup"])
ggplot(lmtp1a$diag, aes(x=eif, group=trt, color=trt)) + geom_density() + theme_bw()

summary(lmtp1c$diag$eif[lmtp1c$diag$trt=="ntx"])
summary(lmtp1c$diag$eif[lmtp1c$diag$trt=="bup"])
ggplot(lmtp1c$diag, aes(x=eif, group=trt, color=trt)) + geom_density() + theme_bw()

summary(lmtp2$diag$eif[lmtp2$diag$trt=="ntx"])
summary(lmtp2$diag$eif[lmtp2$diag$trt=="bup"])
ggplot(lmtp2$diag, aes(x=eif, group=trt, color=trt)) + geom_density() + theme_bw()

# aipw1a <- readRDS(paste0(outpath,"aipw1a_cohort2.rds")) |> data.table()
# last <- aipw1a |> filter(period==26)
# 
# aipw1a[,.(mineif1 = min(eif1),
#                                  maxeif1 = max(eif1),
#                                  mineif0 = min(eif0),
#                                  maxeif0 = max(eif0),
#                                  r1 = mean(eif1),
#                                  se1 = sd(eif1)/sqrt(.N),
#                                  r0 = mean(eif0),
#                                  se0 = sd(eif0)/sqrt(.N),
#                                  rd = mean(eif1-eif0),
#                                  serd = sd(eif1 - eif0)/sqrt(.N),
#                                  rnc = mean(probout)),
#                                  by=.(period)]
# 
# summary(last$eif1)
# summary(last$eif0)

