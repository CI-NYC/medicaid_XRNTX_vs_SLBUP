############################################-
############################################-
# Model for treatment
# Author: Rachael Ross
############################################-
############################################-

#################-
# Libraries----
#################-

library(tidyverse)
library(purrr)

library(mlr3superlearner)
library(mlr3learners)
library(mlr3)
library(mlr3extralearners)

library(tictoc)
library(data.table)
library(future)
library(furrr)
library(progressr)

library(glmnet)
library(MatchIt)
library(Hmisc)

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
dat <- readRDS(paste0(clean,"analysisdat_flat",cohortnum,".rds"))

### Load covariate list
source("/home/rr3551/moudr01/scripts/01_medicaidCE/05_analysis/00_covarlist.R")

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
# LASSO treatment model
#################-

Avec <- dat$ntx

#covars[1] <- "rcs(dem_age)"
#covars[33] <- "rcs(util_oscntex14)"
ageplines <- as.data.frame(Hmisc::rcspline.eval(dat$dem_age, nk=5,inclx = FALSE))
names(ageplines) <- c("dem_agespl1","dem_agespl2","dem_agespl3")
utilsplines <- as.data.frame(Hmisc::rcspline.eval(dat$util_oscntex14, nk=5,inclx = FALSE))
names(utilsplines) <- c("utilspl1","utilspl2","utilspl3")

Wmat <- data.matrix(dat[,c(covars,names(ageplines),names(utilsplines))])

set.seed(73)

# Cross folds to find optimal lambda
cv_models <- cv.glmnet(y = Avec,x = Wmat,alpha=1)

# Optimal lambda by MSE
best_lambda <- cv_models$lambda.min
best_lambda
log(best_lambda)

plot(cv_models)

# Coefficients of best model
lassomod <- glmnet(y=Avec, x=Wmat, alpha=1, lambda = best_lambda)
coef(lassomod)
exp(coef(lassomod))

# For comparison - larger lambda
altlassomod <- glmnet(y=Avec, x=Wmat, alpha=1, lambda = cv_models$lambda.1se)
coef(altlassomod)
exp(coef(altlassomod))



# #################-
# # Matching
# #################-
# 
# # Construct pre-match object - no matching done
# m.out0 <- matchit(reformulate(covars, response = "ntx"), data=dat, method=NULL, distance = "mahalanobis")
# summary(m.out0)
# plot(summary(m.out0))
# 
# # CEM
# cem.out <- matchit(reformulate(covars, response = "ntx"), data=dat, method = "cem", estimand = "ATT", replace=TRUE)
# summary(cem.out)
# 
# # Nearest neighbor
# cem.out <- matchit(reformulate(covars, response = "ntx"), data=dat, method = "nearest", 
#                    distance = "mahalanobis",
#                    estimand = "ATT", replace=TRUE,
#                    caliper = ,# based on propensity score only
#                    ratio = ,
#                    min.controls = ,
#                    max.controls = ,
#                    mahvars = ,# to do mahalanobis matching after caliper implmented on pscore
#                    discard = # only allowed when distance is propensity score
# )
# summary(cem.out)
