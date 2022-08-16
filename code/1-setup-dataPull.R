
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

# File Structure ----------------------------------------------------------

# set up the file strucutre
dirs <- c("code", "data", "documentation", "figures", "function", "output", "year")

for (i in 1:length(dirs)){
  if(dir.exists(dirs[i]) == FALSE){
    dir.create(dirs[i])
  }
}

## ADD DIRS TO GITIGNORE
# just new lines 
# data/
# output/


# {dotenv} setup and secrets ----------------------------------------------

# create a .env file with a value pair for the DB connection string 
# add a line after completed text 

 ## ADD TO GITIGNORE
# just new lines 
# .env

# load .env
load_dot_env(".env") # then access with Sys.getenv("HMS_EDEALER")

# keyring
keyring::key_list("HMS-eDealer")$username
keyring::key_get("HMS-eDealer", "BENJAMINDUFFIN")

# Data pull ---------------------------------------------------------------

# create driver object 
# point this to your local ojdbc8.jar file! 
jdbcDriver <- JDBC(driverClass = "oracle.jdbc.OracleDriver",
                   classPath="C:/instantclient-basic-windows.x64-21.6.0.0.0dbru/instantclient_21_6/ojdbc8.jar") #CHANGE


# Create connection to the database 
jdbConnection <- dbConnect(jdbcDriver, 
                           Sys.getenv("HMS_EDEALER"), 
                           user = keyring::key_list("HMS-eDealer")$username, 
                           password = keyring::key_get("HMS-eDealer", "BENJAMINDUFFIN"))


## Run query 
# for eDealer vessel list 
qry <- "select * from EDEALER.VESSELS"

veslist <- dbGetQuery(jdbConnection, qry)


# save data to data folder 
saveRDS(veslist, here("data", paste0("eDealerVesselList_", Sys.Date(), ".rds")))

# check for open connections and close any 
var <- as.list(.GlobalEnv)
var_names <- names(var)

for (i in seq_along(var_names)){
  if (class(var[[var_names[i]]]) == "JdbConnection"){
    dbDisconnect(var[[var_names[i]]])
  }
}

# remove objects
rm(list = ls())
