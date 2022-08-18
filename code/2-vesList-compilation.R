
# Header ------------------------------------------------------------------

# Compiling vessel list for ACCSP Data Warehouse
# Ben Duffin 8/16/2022
# Updates to existing process to pipeline and include better version control 

#################### LAST UPDATE TO ACCSP WAS 9/14/2021 #####################

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

### creates state FIPS codes from abbreviation using lookup table 
state_code_Fromabr <-function(x) {
  #create an nx1 data.frame of state codes from source column
  st.x<-data.frame(state=x)
  #match source codes with codes from 'st.codes' local variable and use to return the full state name
  refac.x<-st.code$STATE[match(st.x$state,st.code$STATE_ABRV)]
  #return the full state names in the same order in which they appeared in the original source
  return(refac.x)
}

### and vice versa 
state_abr_FromCode <- function(x) {
  st.x <- data.frame(state_code = x)
  refac.x <- st.code$STATE_ABRV[match(st.x$state_code, st.code$STATE)] # add in pad here? 
  return(refac.x)
}

### test if columns are COMPLETELY NA and return vector of those columns 
na_col_test <-  function (x) {
  w <- sapply(x, function(x)all(is.na(x)))
  if (any(w)) {
    print(paste("All NA in columns", paste(which(w), collapse=", ")))
    z <- which(w) 
    return(z)
  }
}

### subsets data based on for only those changed or updated since last submission
veslist_datesub <- function(x) {
  last_date <- as.Date(readLines(here::here("data", "Updates", "date_updated_2021-09-14.csv")))
  subset(x, as.Date(DE) > last_date | as.Date(DC) > last_date)
}


## Convert all to character and to upper 
df_to_upperChar <- function(x) {
  x %>% 
    mutate(across(everything(), ~as.character(.) %>% toupper()))
}

# rectify states based on toupper, trimws, and looking whether or not they have 
# a numeric code that can be converted to state abbreviation
rectify_state <- function(x) {
  # first, we need to trimws and convert toupper 
  x <- toupper(trimws(x))
  
  # then pad numeric ones to two digits of width 2 --- if they aren't a SAFIS vessel 
  x <- ifelse(!grepl("[A-Z]", x), str_pad(x, width = 2, side = "left", pad = "0"), 
              ifelse(grepl("[A-Z]", x), toupper(x), 
                     ifelse(x == "", NA, 
                            x)))
}

colnames_from_file <- function(...) {
  x <- read_xls(here("data", "transfer formats for vessels_i.xls"))
  y <- as.vector(toupper(x$`ACCSP Field Name`))
  y <- y[!is.na(y)]
}


# Load data  --------------------------------------------------------------

# vessel list 
veslist <- readRDS(here("data", paste0("eDealerVesselList_2022-08-16.rds"))) # check for most recent file 

# state FIPS codes/abbreviations
st.code <- read.csv(here::here("data", "state_code.csv"), stringsAsFactors = F)

# var names for ACCSP DW vessel list 
accsp_vars <- colnames_from_file()


## subset for just new vessels that have been updated or added since the last submission of the vessel list 
## Field to Key off: DE for entry date and DC for change date 
ves <- veslist_datesub(veslist)

## Update date 
update_date <- as.Date(readLines(here::here("data", "Updates", "date_updated_2021-09-14.csv")))

# Data prep and validation ------------------------------------------------


# quick fix on padding the string for the code
st.code$STATE <- str_pad(st.code$STATE, side = "left", width = 2, pad = "0")


## Registering state -- want: two letter abbreviation
# will not be fixing the clearly erroneous values
# used for eDealer and to fill same field in ACCSP 

# apply function to non-SAFIS vessels
ves$REGISTERING_STATE_fin <- ifelse(!is.na(ves$SAFIS_VESSEL_ID), rectify_state(ves$REGISTERING_STATE), 
                                 ves$REGISTERING_STATE)

# which numbers didn't convert? 
intersect(unique(ves$REGISTERING_STATE_fin[grepl('[0-9]', ves$REGISTERING_STATE_fin) & !grepl('[A-Z]', ves$REGISTERING_STATE_fin)]), st.code$STATE)
# WHY DIDN'T THESE CONVERT: "20" "49"


# Initialize export dataframe ---------------------------------------------

# create df
a.ves <- data.frame(matrix(NA, nrow = dim(ves)[1], ncol = length(accsp_vars)))
names(a.ves) <- accsp_vars

# build it out 
a.ves <- a.ves %>%
  mutate(SUPPLIER_VESSEL_ID = ves$VESSEL_ID, 
         HULL_ID_NBR = ves$HULL_ID_NBR, 
         COAST_GUARD_NBR = ves$COAST_GUARD_NBR, 
         STATE_REG_NBR = ves$STATE_REG_NBR, 
         REGISTERING_STATE = ves$REGISTERING_STATE_fin,
         DATA_SOURCE = ifelse(is.na(ves$SAFIS_VESSEL_ID), "0032", 
                                    ifelse(ves$SAFIS_VESSEL_ID != ves$VESSEL_ID & !is.na(ves$SAFIS_VESSEL_ID), "0031", 
                                           ifelse(ves$SAFIS_VESSEL_ID == ves$VESSEL_ID, "0031", 
                                                  NA))),
         DATA_SUPPLIER = "0032", 
         GEAR_TYPE = ves$PRIMARY_GEAR, 
         YEAR_BUILT = ves$YEAR_BUILT, 
         SUPPLIER_ACTION_FLAG = ifelse(as.Date(ves$DE) <= update_date & as.Date(ves$DC) > update_date, "U", 
                                       ifelse(as.Date(ves$DE) > update_date & as.Date(ves$DC) > update_date |
                                                        as.Date(ves$DE) > update_date & is.na(ves$DC), "A", 
                                                                ifelse(is.na(ves$DE) & !is.na(ves$DC), "U", 
                                                                       NA))), 
         SAFIS_VESSEL_ID = ves$SAFIS_VESSEL_ID,
         HAS_SAFIS_VESSEL_ID = ifelse(is.na(ves$SAFIS_VESSEL_ID), "N", "Y")
         )

# check NA
table(is.na(a.ves$SUPPLIER_ACTION_FLAG)) # one NA? 

table(a.ves$SUPPLIER_ACTION_FLAG) # good, matches eDealer

table(a.ves$HAS_SAFIS_VESSEL_ID)

# Check max chars for each 
lapply(a.ves, function(x) max(nchar(x), na.rm = T))


# Convert all to uppercase characters
a.ves <- df_to_upperChar(a.ves)

# remove punctuation 
a.ves <- a.ves %>% 
  mutate(across(c(STATE_REG_NBR, HULL_ID_NBR, COAST_GUARD_NBR, REGISTERING_STATE), ~gsub('\\.', '', .x)))


str(a.ves)

# BD 8/18/2022- with a small subset of data, we don't need this I assume
  # Will leave it here for now in case that changes #
# UPDATES AFTER EMAIL FROM Heather K. 5/2021 #### 

## REMOVE COLUMNS THAT ARE ALL NA 
# keepvars <- names(a.ves)[sapply(a.ves1, function(x) all(is.na(x))) == F]
# 
# na.test(a.ves1)
# 
# 
# (dropvars <- names(a.ves1)[c( 4, 10, 11, 12, 14, 15, 16, 17, 19, 20, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41)]) # looks good 
# 
# 
# keepvars <- names(a.ves1)[!(names(a.ves1) %in% dropvars1)]
# 
# a.ves1 <- subset(a.ves1, select = keepvars)


## CHECK LENGTH OF REGISTERTING STATE 
table(nchar(a.ves$REGISTERING_STATE)) # hmm, only 1 or 2? HK said something about some being longer, not seeing it 

table(a.ves1$REGISTERING_STATE)


# Date cutoff setup for next run ------------------------------------------

# write csv
writeLines(as.character(Sys.Date()), here::here("data", "Updates", paste0("date_updated_", Sys.Date(), ".csv")))

# check ### This still needs work 
if(any(as.Date(readLines(here::here("data", "Updates", "date_updated.csv"))) < Sys.Date())) {
  print("Data not up to date") 
} else {
  print("Data up to date")
}



# Write file --------------------------------------------------------------

write_xlsx(a.ves, here::here("output", paste0("eDealer_vessel_list_accsp_upload_", Sys.Date(), ".xlsx")))


# Misc things to check  ---------------------------------------------------
################### Look at what was done before 
# registering state - two letter abbreviations
# replace "bad" records in the registering state field with the state reg abr? 

# looked at duplicated SUPPLIER_VESSEL_ID
veslist$SUPPLIER_VESSEL_ID[duplicated(veslist$SUPPLIER_VESSEL_ID)] # "NJ7001HN" "1317763" 


# oddballs with preceding 0's
table(veslist$SUPPLIER_VESSEL_ID[is.na(veslist$COAST_GUARD_NBR) & is.na(veslist$STATE_REG_NBR)]) # 1 with XXXXXXX

# how many have letters for beginning 
table(grepl("[A-Z]", substr(veslist$SUPPLIER_VESSEL_ID[is.na(veslist$COAST_GUARD_NBR) & is.na(veslist$STATE_REG_NBR)], 1, 2)))
 
# just the 1 XXXXXXXXXX
subset(veslist, is.na(COAST_GUARD_NBR) & is.na(STATE_REG_NBR))

# one with invalid multibye string 
table(nchar(veslist$VESSEL_NAME))
veslist$VESSEL_NAME[veslist$VESSEL_ID == '175078']

# how many without coast guard or state reg #
table(is.na(veslist$COAST_GUARD_NBR), is.na(veslist$STATE_REG_NBR)) # oka, just that one from above 

# leading 0 fixed?
ves[grepl("^0+", ves$SUPPLIER_VESSEL_ID), ] # looks like there are some that need to be addressed

# Leading "DO-" fixed? 
ves[grepl("DO", ves$SUPPLIER_VESSEL_ID), ] # one vessel 

#DOprefix fixed, 
#check: 
  # the safis Y/N indicator
  # EMailS and State
  # NA registering state - potentiall from the state reg, state_code...


