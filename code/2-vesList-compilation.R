
# Header ------------------------------------------------------------------

# Compiling vessel list for ACCSP Data Warehouse
# Ben Duffin 8/16/2022
# Updates to existing process to pipeline and include better version control 


# Libraries ---------------------------------------------------------------

library(RJDBC)
library(dotenv)
library(keyring)
library(writexl)
library(readxl)
library(plyr)
library(dplyr)
library(stringr)
library(targets)
library(here)


# Load functions ----------------------------------------------------------

## creates state FIPS codes from abbreviation using lookup table 
statecodeFromabr <-function(x) {
  #create an nx1 data.frame of state codes from source column
  st.x<-data.frame(state=x)
  #match source codes with codes from 'st.codes' local variable and use to return the full state name
  refac.x<-st.code$STATE[match(st.x$state,st.code$STATE_ABRV)]
  #return the full state names in the same order in which they appeared in the original source
  return(refac.x)
}

## and vice versa 
stateAbrFromCode <- function(x) {
  st.x <- data.frame(state_code = x)
  refac.x <- st.code$STATE_ABRV[match(st.x$state_code, st.code$STATE)]
  return(refac.x)
}

# Load data  --------------------------------------------------------------

# vessel list 
veslist <- readRDS(here("data", paste0("eDealerVesselList_", Sys.Date(), ".rds")))

# state FIPS codes/abbreviations
st.code <- read.csv(here::here("data", "state_code.csv"), stringsAsFactors = F)

## subset for just new vessels that have been updated or added since the last submission of the vessel list 
