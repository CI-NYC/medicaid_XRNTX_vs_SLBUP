############################################
############################################
# Survival plots of results
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

#################
# Paths
#################

# Local
home <- Sys.getenv("HOME")
path <- paste0(home, "/07 Postdoc/02 Projects/02 Medicaid CE/03 Output")
setwd(path)

#################
# Load analysis results files
#################

cohortnum <- 2
mycolors <- c("#2e4057",
              "#66a182")
mylinetypes <- c("solid","31")

## Risk data
# Crude results by week
crude_wks_ <- readRDS(paste0("crisk_wks_cohort",cohortnum,".rds")) 
levels(crude_wks_$ntx) <- c("SL-BUP","XR-NTX")

crude_wks <- crude_wks_ |>
  mutate(day = t*7-6) |> # create a day variable so can use day for x-axis
  rbind(crude_wks_ |> filter(t==26) |> mutate(day = 180)) |>
  arrange(out,ntx,day) 

# Adjusted results by week
lmtp1a <- readRDS(paste0("lmtpall1a_cohort",cohortnum,".rds")) |> mutate(out="1a")
lmtp1c <- readRDS(paste0("lmtpall1c_cohort",cohortnum,".rds"))  |> mutate(out="1c")
lmtp2 <- readRDS(paste0("lmtpall2_cohort",cohortnum,".rds"))  |> mutate(out="2")

adj_wks_ <- rbind(lmtp1a,lmtp1c,lmtp2) |>
  filter(trt!="rd") |>
  rename(r = est, r_lcl = lcl, r_ucl = ucl) |>
  mutate(ntx = ifelse(trt=="bup","SL-BUP","XR-NTX")) |>
  dplyr::select(-trt) %>%
  rbind(.,crude_wks |> 
          filter(t==2 & out!="1b") |> 
          dplyr::select(t,ntx,out,se,r,r_lcl,r_ucl)) #add a row so that curves start at 0

adj_wks <- adj_wks_ |>
  mutate(day = t*7-6) |> # create a day variable so can use day for x-axis
  rbind(adj_wks_ |> filter(t==26) |> mutate(day = 180)) |>
  arrange(out,ntx,day) |>
  mutate(r=ifelse(r<0.000001,0,r))

## Risk difference data
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

# Crude
cruderds_wks_ <- getrds(crude_wks_)
cruderds_wks <- cruderds_wks_ |>
  mutate(day = t*7-6) |> # create a day variable so can use day for x-axis
  rbind(cruderds_wks_ |> filter(t==26) |> mutate(day = 180)) |>
  arrange(out,day) 

adjrd_wks_ <- rbind(lmtp1a,lmtp1c,lmtp2) |>
  filter(trt=="rd") |>
  rename(rd = est, rd_lcl = lcl, rd_ucl = ucl) |>
  dplyr::select(-trt) %>%
  rbind(.,cruderds_wks |> 
          filter(rd==0 & out!="1b") |> 
          dplyr::select(t,out,se,rd,rd_lcl,rd_ucl))

adjrd_wks <- adjrd_wks_ |>
  mutate(day = t*7-6) |> # create a day variable so can use day for x-axis
  rbind(adjrd_wks_ |> filter(t==26) |> mutate(day = 180)) |>
  arrange(out,day) 


#################
# Functions and making plots
#################

### Risks
riskplot <- function(data,outcome,rmax,rbreaks){
  crude <- grepl("crude",deparse(substitute(data)))

  fig <- data|>
    mutate(r = r*100,
           r_lcl = r_lcl*100,
           r_ucl = r_ucl*100) |>
    filter(out==outcome) |>
    ggplot() +
      theme_classic() +
    if(crude==TRUE){
      theme(legend.position = c(0.2, 0.8),
            legend.title = element_blank(),
            legend.box.background = element_rect(colour = "black"),
            #legend.background = element_rect(fill="transparent"),
            legend.background = element_blank(),
            panel.background = element_rect(fill = NA),
            panel.grid.major = element_line(colour = "gray94"),
            legend.margin=margin(c(1,5,5,5)))
    }else{
      theme(legend.position = "none",
            panel.background = element_rect(fill = NA),
            panel.grid.major = element_line(colour = "gray94"))
    }
    fig + geom_stepconfint(aes(x = day, ymin = r_lcl, ymax = r_ucl,
                      group = ntx,
                      fill = ntx)
                      ,
                  alpha=0.3) +
      geom_step(aes(x = day, y = r,
                    group = ntx,
                    color = ntx, linetype = ntx)
                ,
                show.legend = TRUE,
                linewidth = 1) +
      xlab("Time (days)")+
      ylab("Cumulative risk (%)") +
      scale_x_continuous(limits = c(0, 180),
                         breaks = seq(0,180,by=30),
                         expand = expansion(add = c(0, 3)))+
      scale_y_continuous(limits = c(0, rmax),
                         breaks = rbreaks,
                         expand = c(0, 0)) +
      scale_color_manual(values = mycolors,
                         breaks=c("XR-NTX","SL-BUP")) +
      scale_fill_manual(values = mycolors,
                        breaks=c("XR-NTX","SL-BUP")) +
      scale_linetype_manual(values = mylinetypes,
                            breaks=c("XR-NTX","SL-BUP"))
    #theme(axis.text.x = element_text(angle = -45))#, vjust = 0.5, hjust=1))
} 

cr1a <- riskplot(crude_wks,"1a",100,seq(0,100,by=20))
ar1a <- riskplot(adj_wks,"1a",100,seq(0,100,by=20))

cr1c <- riskplot(crude_wks,"1c",100,seq(0,100,by=20))
ar1c <- riskplot(adj_wks,"1c",100,seq(0,100,by=20))

cr2 <- riskplot(crude_wks,"2",7,seq(0,6,by=2))
ar2 <- riskplot(adj_wks,"2",7,seq(0,6,by=2))



### Risk difference

rdplot <- function(data,outcome,rdmin,rdmax,rdbreaks){

  data|>
    filter(out==outcome) |>
    mutate(rd = rd*100,
           rd_lcl = rd_lcl*100,
           rd_ucl = rd_ucl*100) |>
  ggplot() +
    theme_classic() +
    theme(panel.background = element_rect(fill = NA),
          panel.grid.major = element_line(colour = "gray94"),
          legend.margin=margin(c(1,5,5,5))) +
    geom_hline(yintercept=0) + 
    geom_stepconfint(aes(x = day, ymin = rd_lcl, ymax = rd_ucl),
                alpha=0.3,
                fill = "#2e4057") +
    geom_step(aes(x = day, y = rd),
              show.legend = FALSE,
              linewidth = 1) +
    xlab("Time (days)")+
    ylab("Risk difference (% pts)") +
    scale_x_continuous(limits = c(0,180),
                       breaks = seq(0,180,by=30),
                       expand = expansion(add = c(0, 3)))+
    scale_y_continuous(limits = c(rdmin, rdmax), 
                       breaks = rdbreaks,
                       expand = c(0, 0)) +
    annotate(geom="text", x=15, y=(.75*rdmax), 
             label="XR-NTX has",
             size=3.5,
             color="black") +
    annotate(geom="text", x=15, y=(.75*rdmax - .15*rdmax), 
             label="higher risk",
             size=3.5,
             color="black") +
    annotate(geom="text", x=15, y=(.75*rdmin - .15*rdmin),
             label="SL-BUP has",
             size=3.5,
             color="black") +
    annotate(geom="text", x=15, y=(.75*rdmin),
             label="higher risk",
             size=3.5,
             color="black") 
  
}

ard1a <- rdplot(adjrd_wks,"1a",-21,21,seq(-20,20,by=10))
ard1c <- rdplot(adjrd_wks,"1c",-21,21,seq(-20,20,by=10))
ard2 <- rdplot(adjrd_wks,"2",-2,2,seq(-2,2,by=1))

### Combined figures

plots1a <- (cr1a | ar1a)/ard1a + plot_annotation(tag_levels = "A") 
plots1c <- (cr1c | ar1c)/ard1c + plot_annotation(tag_levels = "A")
plots2 <- (cr2 | ar2)/ard2 + plot_annotation(tag_levels = "A")



savefig <- function(fig){
  name <- deparse(substitute(fig))
  ggsave(paste0(path,"/figures/",name,".png"),fig,
         units = "in",width = 8,height = 6.5,dpi = 600)
}
savefig(plots1a)
savefig(plots1c)
savefig(plots2)


