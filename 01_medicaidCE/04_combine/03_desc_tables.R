############################################-
############################################-
# Create descriptive tables
# Author: Rachael Ross
############################################-
############################################-

#################-
# Libraries----
#################-
library(tidyverse)
library(gtsummary)
library(gt)
library(tableone)

#################-
# Paths/load data----
#################-

projpath <- "/home/data/moud/01_medicaidCE/"

# Full
path <- paste0(projpath,"intermediate/")
clean <- paste0(projpath,"clean/")

# # Sample
# path <- paste0(projpath,"sample/intermediate/")
# clean <- paste0(projpath,"sample/clean/")

# Output
outpath <- "/home/rr3551/moudr01/output/01_medicaidCE/"

# Cohort number
cohortnum <- 2

# load data
cohort <- readRDS(paste0(clean,"analysisdat_flat",cohortnum,".rds")) |>
  mutate(moud = factor(ifelse(moud=="ntx","XR-NTX",
                       ifelse(moud=="mth","MTH","O-BUP")))) |>
  mutate(moud_pxany = case_when(moud_px_tot_cnt>0 ~ 1, .default = 0),
         moud_ndcany = case_when(moud_ndc_tot_cnt>0 ~ 1, .default = 0),
         moud_type = factor(case_when(moud_pxany==1 & moud_ndcany==0 ~ "Procedure",
                             moud_pxany==0 & moud_ndcany==1 ~ "NDC",
                             .default = "Both")),
         moud_rxos = case_when(moud_ndc_rx_cnt==0 ~ "OS only",
                               moud_ndc_tot_cnt>moud_ndc_rx_cnt | moud_px_tot_cnt>0 ~ "Both",
                               .default = "RX only"),
         moud_osonly = ifelse(moud_rxos=="OS only",1,0)) 
  
cohort$moud <- factor(cohort$moud, levels=c("XR-NTX","O-BUP"))
levels(cohort$dem_re)[5] <- "Hawaiian/Pacific Islander"
cohort$dem_re <- factor(cohort$dem_re, levels=c("White, non-Hispanic","Black, non-Hispanic","Hispanic, all races",
                                                "Asian, non-Hispanic","AIAN, non-Hispanic","Hawaiian/Pacific Islander"))

sort(names(cohort))

#################-
# Meds to include in tables----
#################-

meds <- c("XR-NTX","O-BUP")

#################-
# Make labels----
#################-

list_of_labels <- list(STATE = "State",
                       dem_age = "Age, median (IQR)",
                       dem_male = "Male",
                       dem_re = "Race/ethnicity",
                       dem_married = "Married/partnered",
                       dem_english = "English primary language",
                       dem_hshold_cat = "Household size",
                       dem_poverty = "Income <= 133% Poverty", 
                       dem_citizen = "US citizen",
                       dem_tanf = "TANF benefits",
                       dem_ssi = "SSI benefits",
                       sud_alcohol = "Alcohol",
                       sud_cannabis = "Cannabis",
                       sud_sedative = "Sedatives",
                       sud_cocaine = "Cocaine",
                       sud_amphetamine = "Amphetamine",
                       sud_other = "Other",
                       sud_stimod = "Stimulant overdose",
                       sud_alcoholalt = "Alcohol",
                       sud_cannabisalt = "Cannabis",
                       sud_sedativealt = "Sedatives",
                       sud_cocainealt = "Cocaine",
                       sud_amphetaminealt = "Amphetamine",
                       sud_otheralt = "Other",
                       sud_stimodalt = "Stimulant overdose",
                       psy_depression = "Depression",
                       psy_psych = "Schizophrenia/psychosis",
                       psy_anxiety = "Anxiety",
                       psy_bipolar = "Bipolar",
                       psy_adhd = "ADD/ADHD",
                       psy_ptsd = "PTSD",
                       psy_psychoth = "Other",
                       psy_depressionalt = "Depression",
                       psy_psychalt = "Schizophrenia/psychosis",
                       psy_anxietyalt = "Anxiety",
                       psy_bipolaralt = "Bipolar",
                       psy_adhdalt = "ADD/ADHD",
                       psy_ptsdalt = "PTSD",
                       psy_psychothalt = "Other",
                       com_disability = "Disability",
                       com_disabilityalt = "Disability",
                       com_chronicpn = "Chronic pain",
                       oud_inpatient = "Inpatient hospitalization",
                       oud_therapy = "Psychosocial/behavioral therapy",
                       oud_inpatientalt = "Inpatient hospitalization",
                       oud_therapyalt = "Psychosocial/behavioral therapy",
                       oud_overdose30 = "Overdose, past 30 days",
                       oud_overdose60 = "Overdose, past 60 days",
                       oud_overdose_cat = "Overdose, count",
                       oud_overdoseany = "Overdose",
                       oud_overdose_catalt = "Overdose, count",
                       oud_overdoseanyalt = "Overdose",
                       med_benzos = "Benzodiazepines",
                       med_antidep = "Antidepressants",
                       med_antipsych = "Antipsychotics",
                       med_stims = "Stimulants",
                       med_gaba = "Gabapentinoids",
                       med_clon = "Imidazoline receptor agonists",
                       med_ntxoral = "Oral naltrexone",
                       med_benzosalt = "Benzodiazepines",
                       med_antidepalt = "Antidepressants",
                       med_antipsychalt = "Antipsychotics",
                       med_stimsalt = "Stimulants",
                       med_gabaalt = "Gabapentinoids",
                       med_clonalt = "Imidazoline receptor agonists",
                       med_ntxoralalt = "Oral naltrexone",
                       util_ipcnt = "Inpatient, count",
                       util_ipany = "Inpatient hospitalization",
                       util_oscnt = "Other services encounters, count",
                       util_oscntex14 = "Other services encounters, count",
                       util_edcnt = "ED, count",
                       util_edcntex14 = "ED >14d prior, count",
                       util_ipcntalt = "Inpatient, count",
                       util_ipanyalt = "Inpatient hospitalization",
                       util_oscntalt = "Other services encounters, median(IQR)",
                       util_oscntex14alt = "Other services encounters, median(IQR)",
                       util_edcntalt = "ED, count",
                       util_edcntex14alt = "ED >14d prior, count",
                       util_edany = "Emergency department encounter",
                       util_edanyalt = "Emergency department encounter",
                       total = "N",
                       moud = "Treatment",
                       moud_both = "Both px/ndcs",
                       moud_miss_days = "Missing days",
                       moud_ndc_ed_cnt = "NDC, ED codes",
                       moud_ndc_ed_days = "NDC, ED days",
                       moud_ndc_ip_cnt = "NDC, IP codes",
                       moud_ndc_ip_days = "NDC, IP days",
                       moud_ndc_os_cnt = "NDC, OS codes",
                       moud_ndc_os_days = "NDC, OS days",
                       moud_ndc_rx_cnt = "NDC, RX codes",
                       moud_ndc_rx_days = "NDC, RX days",
                       moud_ndc_tot_days = "NDC, total days",
                       moud_ndc_tot_cnt = "NDC, total codes",
                       moud_px_ed_cnt = "PX, ED codes",
                       moud_px_ed_days = "PX, ED days",
                       moud_px_ip_cnt = "PX, IP codes",
                       moud_px_ip_days = "PX, IP days",
                       moud_px_os_cnt = "PX, OS codes",
                       moud_px_os_days = "PX, OS days",
                       moud_px_tot_days = "PX, total days",
                       moud_px_tot_cnt = "PX, total codes",
                       moud_pxany = "Any PX codes",
                       moud_ndcany = "Any NDC codes",
                       moud_type = "Type",
                       moud_rxos = "Source",
                       moud_tot_days = "Total days",
                       event2_days = "Person-days",
                       event2_overdose = "Overdose",
                       event2_death = "Death",
                       event2_delta = "Either")


#################-
# Subset variables of interest----
#################-

### Baseline covariates

dem_vars <- c("dem_age",
          "dem_male",
          "dem_re",
          #"dem_married",
          #"dem_english",
          #"dem_citizen",
          #"dem_hshold_cat",         
          #"dem_poverty",
          "dem_tanf",
          "dem_ssi",
          "STATE")

com_vars <- c("com_disability",
              "com_chronicpn")
com_vars_alt <- c("com_disabilityalt")

psy_vars <- c("psy_depression",
              "psy_psych",
              "psy_anxiety",
              "psy_bipolar",
              "psy_adhd",
              "psy_ptsd",
              "psy_psychoth")
psy_vars_alt <- paste0(psy_vars,"alt") 

sud_vars <- c("sud_alcohol",
              "sud_cannabis",
              "sud_sedative",
              "sud_cocaine",
              "sud_amphetamine",
              "sud_other"
              #,
              #"sud_stimod"
              )
sud_vars_alt <- paste0(sud_vars,"alt")

oud_vars <- c("oud_inpatient",
              "oud_therapy",
              #"oud_overdose30",
              #"oud_overdose60",
              "oud_overdoseany")
oud_vars_alt <- c("oud_inpatient",
              "oud_therapyalt",
              #"oud_overdose30",
              #"oud_overdose60",
              "oud_overdoseanyalt")

med_vars <- c("med_benzos",
              "med_antidep",
              "med_antipsych",
              "med_stims",
              "med_gaba",
              "med_clon"
              #,
              #"med_ntxoral"
              )
med_vars_alt <- paste0(med_vars,"alt")


util_vars <- c("util_ipany",
               "util_edany",
               #"util_oscnt",
               "util_oscntex14")
util_vars_alt <- paste0(util_vars,"alt")


### Baseline MOUD variables

moud_vars <- c("moud_type",
               #"moud_both",
               #"moud_pxany",
               #"moud_ndcany",
               "moud_tot_days",
               "moud_miss_days",
               "moud_rxos")

moudpx_vars <- c("moud_px_ip_cnt",
                 "moud_px_os_cnt",
                 "moud_px_ed_cnt",
                 "moud_px_tot_cnt",
                 "moud_px_ip_days",
                 "moud_px_os_days",
                 "moud_px_ed_days",
                 "moud_px_tot_days")

moudndc_vars <- c("moud_ndc_rx_cnt",
                  "moud_ndc_ip_cnt",
                  "moud_ndc_os_cnt",
                  "moud_ndc_ed_cnt",
                  "moud_ndc_tot_cnt",
                  "moud_ndc_rx_days",
                  "moud_ndc_ip_days",
                  "moud_ndc_os_days",
                  "moud_ndc_ed_days",
                  "moud_ndc_tot_days")



################# -
# Table of missingness ----
################# -


makemisstbl <- function(by,meds){
  cohort |>
    filter(moud %in% meds) |>
    select(all_of(by),
           all_of(dem_vars)) |>
    mutate(across(all_of(dem_vars), ~ case_when(is.na(.x) ~ 1, TRUE ~ 0))) |>
    gtsummary::tbl_summary(
      by = by,
      label = list_of_labels) |>
    #modify_spanning_header(all_stat_cols() ~ "**label, N(%)**") |>
    #bold_labels() |>
    modify_header(all_stat_cols() ~ "**{level}**<br>N = {n}") |>
    add_overall(last=TRUE,
                col_label = "**Overall**<br>N = {n}") |>
    modify_footnote(c(all_stat_cols()) ~ NA)
}

tbl_miss <- makemisstbl("STATE", meds)
#tbl_miss <- makemisstbl("YEAR")

tbl_miss

# Function for getting latex
getlatex <- function(input){
  input |>
    as_latex() |>
    as.character() |>
    cat()
}


getlatex(as_gt(tbl_miss))

#################-
# Table 1 ----
#################-

# Function for subsections
subtbls <- function(vars, meds){
  cohort |>
    filter(moud %in% meds) |>
    select(moud,
           all_of(vars),) |>
    gtsummary::tbl_summary(
      by = moud,
      label = list_of_labels) |>
    modify_spanning_header(all_stat_cols() ~ "**Treatment, N(%)**") |>
  modify_header(all_stat_cols() ~ "**{level}**<br>N = {n}") |>
    add_overall(last=TRUE,
                col_label = "**Overall**<br>N = {n}") |>
    modify_footnote(c(all_stat_cols()) ~ NA) 
}

levels(cohort$dem_re)

##### Demographics section
dem <- subtbls(dem_vars,meds)
dem
#getlatex(as_gt(dem))

##### Clinical variables

runsubtbls <- function(alt=FALSE){
  
psy <- subtbls(if(alt==FALSE){psy_vars}else{psy_vars_alt},meds)
com <- subtbls(if(alt==FALSE){com_vars}else{com_vars_alt},meds)
sud <- subtbls(if(alt==FALSE){sud_vars}else{sud_vars_alt},meds)
oud <- subtbls(if(alt==FALSE){oud_vars}else{oud_vars_alt},meds)
med <- subtbls(if(alt==FALSE){med_vars}else{med_vars_alt},meds)
util <- subtbls(if(alt==FALSE){util_vars}else{util_vars_alt},meds)

tbl1 <- tbl_stack(list(dem,
                       oud,
                       sud,
                       psy,
                       com,
                       med,
                       util),
                  group_header = c("Demographics",
                  "OUD characteristics",
                                   "Other substance use disorders",
                                   "Psychiatric comorbid conditions",
                                   "Other comorbid conditions",
                                   "Medications",
                                   "Healthcare utilization (encounters)")) %>%
  as_gt() %>%
  gt::tab_style(
    style = gt::cell_text(style = "italic"),
    locations = gt::cells_row_groups(groups = everything()))

return(tbl1)
}

tbl1 <- runsubtbls(FALSE)   # False for cohorts 1, 2, 4, 5 (check this)

tbl1
getlatex(tbl1)

## For standardized mean differences
getstdmdiff <- function(vars){
  print(CreateTableOne(vars = vars, strata = "moud", data=cohort, test=FALSE)
        , smd=TRUE)
  }

getstdmdiff(dem_vars)
getstdmdiff(oud_vars)
getstdmdiff(sud_vars)
getstdmdiff(psy_vars)
getstdmdiff(com_vars)
getstdmdiff(med_vars)
getstdmdiff(util_vars)


#################-
# Table of initial moud ----
#################-

# Overall
moudtbl <- subtbls(moud_vars,meds)
moudtbl
getlatex(as_gt(moudtbl)) 

cohort[,.N,by=.(moud,moud_tot_days)][order(moud,moud_tot_days)]
cohort[moud_tot_days>100]
summary(cohort$moud_tot_days[cohort$moud=="O-BUP"])
# Add something here for distirbution of BUP duration

# To look at NDC and PX separately
subtbls_moud <- function(vars,meds,type){
  cohort[cohort[[type]]==1,] |>
    filter(moud %in% meds) |>
    select(moud,
           all_of(vars),) |>
    gtsummary::tbl_summary(
      by = moud,
      label = list_of_labels) |>
    modify_spanning_header(all_stat_cols() ~ "**Treatment, N(%)**") |>
    modify_header(all_stat_cols() ~ "**{level}**<br>N = {n}") |>
    modify_footnote(c(all_stat_cols()) ~ NA)
}
table(cohort$moud_ndcany)
moudndctbl <- subtbls_moud(moudndc_vars,meds,"moud_ndcany")
moudpxtbl <- subtbls_moud(moudpx_vars,meds,"moud_pxany")

getlatex(as_gt(moudndctbl))
getlatex(as_gt(moudpxtbl))

# Look at other services only claims
subtbls_moud(moudndc_vars,meds,"moud_osonly")
subtbls_moud(moudpx_vars,meds,"moud_osonly")

#################-
# Table of outcomes ----
#################-

# Event 1a
event1a <- cohort |>
  filter(moud %in% meds) |>
  group_by(moud) |>
  summarise(discontinue = sum(event1a_discon),
            overdose = NA,
            death = sum(event1a_death),
            delta = sum(event1a_delta),
            ltfu = sum(event1a_ltfu),
            days = sum(event1a_days)) |>
  mutate(yrs = days/365.25,
         rate = delta/yrs * 10) |>
  mutate(grp="1a")

event1atot <- cohort |>
  filter(moud %in% meds) |>
  summarise(discontinue = sum(event1a_discon),
            overdose = NA,
            death = sum(event1a_death),
            delta = sum(event1a_delta),
            ltfu = sum(event1a_ltfu),
            days = sum(event1a_days)) |>
  mutate(yrs = days/365.25,
         rate = delta/yrs * 10,
         moud = "Total") |>
  mutate(grp="1a")

# Event 1b
event1b <- cohort |>
  filter(moud %in% meds) |>
  group_by(moud) |>
  summarise(discontinue = sum(event1b_discon),
            overdose = NA,
            death = sum(event1b_death),
            delta = sum(event1b_delta),
            ltfu = sum(event1b_ltfu),
            days = sum(event1b_days)) |>
  mutate(yrs = days/365.25,
         rate = delta/yrs * 10) |>
  mutate(grp="1b")

event1btot <- cohort |>
  filter(moud %in% meds) |>
  summarise(discontinue = sum(event1b_discon),
            overdose = NA,
            death = sum(event1b_death),
            delta = sum(event1b_delta),
            ltfu = sum(event1b_ltfu),
            days = sum(event1b_days)) |>
  mutate(yrs = days/365.25,
         rate = delta/yrs * 10,
         moud = "Total") |>
  mutate(grp="1b")

# Event 1c
event1c <- cohort |>
  filter(moud %in% meds) |>
  group_by(moud) |>
  summarise(discontinue = sum(event1c_discon),
            overdose = NA,
            death = sum(event1c_death),
            delta = sum(event1c_delta),
            ltfu = sum(event1c_ltfu),
            days = sum(event1c_days)) |>
  mutate(yrs = days/365.25,
         rate = delta/yrs * 10) |>
  mutate(grp="1c")

event1ctot <- cohort |>
  filter(moud %in% meds) |>
  summarise(discontinue = sum(event1c_discon),
            overdose = NA,
            death = sum(event1c_death),
            delta = sum(event1c_delta),
            ltfu = sum(event1c_ltfu),
            days = sum(event1c_days)) |>
  mutate(yrs = days/365.25,
         rate = delta/yrs * 10,
         moud = "Total") |>
  mutate(grp="1c")

# Event 2
event2 <- cohort |>
  filter(moud %in% meds) |>
  group_by(moud) |>
  summarise(discontinue = NA,
            overdose = sum(event2_overdose),
            death = sum(event2_death),
            delta = sum(event2_delta),
            ltfu = sum(event2_ltfu),
            days = sum(event2_days)) |>
  mutate(yrs = days/365.25,
         rate = delta/yrs * 10) |>
  mutate(grp="2")

event2tot <- cohort |>
  filter(moud %in% meds) |>
  summarise(discontinue = NA,
            overdose = sum(event2_overdose),
            death = sum(event2_death),
            delta = sum(event2_delta),
            ltfu = sum(event2_ltfu),
            days = sum(event2_days)) |>
  mutate(yrs = days/365.25,
         rate = delta/yrs * 10,
         moud = "Total") |>
  mutate(grp="2")

# Combine

tbl_out <- rbind(event1a,event1atot,
                 event1b,event1btot,
                 event1c,event1ctot,
                 event2,event2tot) |>
  gt() |>
  cols_hide(columns = c(days,grp)) |>
  tab_row_group(
    label = "Discontinuation (a) or death",
    rows = grp=="1a",
    id = "1a"
  ) |> 
  tab_row_group(
    label = "Discontinuation (b) or death",
    rows = grp=="1b",
    id = "1b"
  ) |>
  tab_row_group(
    label = "Discontinuation (c) or death",
    rows = grp=="1c",
    id = "1c"
  ) |>
  tab_row_group(
    label = "Overdose or death",
    rows = grp=="2",
    id = "2"
  ) |>
  row_group_order(groups = c("1a","1b","1c","2")) |>
  cols_label(moud = "Treatment",
             discontinue = "Discontinue",
             overdose = "Overdose",
             death = "Death",
             delta = "Total events",
             ltfu = "LTFU",
             yrs = "Person-years",
             rate = "Rate") |>
  fmt_integer() |>
  fmt_number(columns = yrs,
             decimals = 0,
             use_seps = TRUE) |>
  fmt_number(columns = rate, 
             decimals = 2) |>
  gt::tab_style(
    style = gt::cell_text(style = "italic"),
    locations = gt::cells_row_groups(groups = everything()))
  
getlatex(tbl_out)

6752/11641

# When is the last outcome in each arm
setDT(cohort)[event2_delta==0,.(maxdeath=max(event2_wks)),by=.(moud)]

