library(dplyr)
library(datapkg)
library(data.table)
library(stringr)
library(reshape2)

##################################################################
#
# Processing Script for Births
# Created by Jenna Daly
# On 06/19/2017
#
##################################################################

#Setup environment
sub_folders <- list.files()
raw_data_location <- grep("raw", sub_folders, value=T)
data_location <- grep("data$", sub_folders, value=T)
path_to_data <-  (paste0(getwd(), "/", data_location))
path_to_raw_data <- (paste0(getwd(), "/", raw_data_location))
table3_csvs <- dir(path_to_raw_data, recursive=T, pattern = "byTOWNrev")

# Read in data
raw_99_13 <- fread(paste0(path_to_raw_data, "/", table3_csvs[2]), colClasses="character")
raw_14 <- fread(paste0(path_to_raw_data, "/", table3_csvs[1]), colClasses="character")

#combine old and new data
raw <- rbind(raw_99_13, raw_14)

# join to labels relationship table to replace LABEL column with something more useful
labels <- fread(paste0(path_to_raw_data, "/", "labels.csv"), colClasses="character")
setkey(labels, GROUPDEX, LABEL)
setkey(raw, GROUPDEX, LABEL)
raw <- labels[raw, mult="first"]

# cleanup
remove(labels)

# reset key of raw to all columns
setkey(raw)

# override base as.numeric functionality with process specific to this set.
as.numeric <- function(x, ...) {
  x <- gsub(",", "", x, fixed=T)
  x <- ifelse(
    !is.na(base::as.numeric(x)),
    x,
    "0"
  )
  return(base::as.numeric(x));
}

# clean and process set before reshaping
raw[] <- lapply(raw, gsub, pattern=',', replacement='')

raw_clean <- raw %>% 
  mutate(GEOG_name = str_to_title(GEOG_name), 
         AGGREGATION = str_replace(AGGREGATION, "YR", "-Year"), 
         RR_YR = str_replace(RR_YR, ":", "-"),
         BIRTHS = as.numeric(BIRTHS),
         BWu500 = as.numeric(BWu500),
         BW500 = as.numeric(BW500),
         BW1000 = as.numeric(BW1000),
         BW1500 = as.numeric(BW1500),
         BW2500 = as.numeric(BW2500),
         BW3500 = as.numeric(BW3500),
         BW_UNK = as.numeric(BW_UNK),
         pctVLBW = as.numeric(pctVLBW),
         pctLBW = as.numeric(pctLBW),
         PRETERM = as.numeric(PRETERM),
         TERM = as.numeric(TERM),
         TERM_UNK = as.numeric(TERM_UNK),
         pctPREM = as.numeric(pctPREM), 
         VLBW = BWu500 + BW500 + BW1000,
         LBW = BWu500 + BW500 + BW1000 + BW1500,
         AllTerm = PRETERM + TERM + TERM_UNK) %>% 
  rename(Town = GEOG_name, 
         Year = RR_YR, 
         Aggregation = AGGREGATION, 
         Column = fixedColumn,
         Level = fixedLevel,
         Births = BIRTHS, 
         PreTerm = PRETERM, 
         Term = TERM, 
         UnkTerm = TERM_UNK,
         P_Preterm = pctPREM,
         P_VLBW = pctVLBW, 
         P_LBW = pctLBW)

#Merge in FIPS
town_fips_dp_URL <- 'https://raw.githubusercontent.com/CT-Data-Collaborative/ct-town-list/master/datapackage.json'
town_fips_dp <- datapkg_read(path = town_fips_dp_URL)
town_fips <- (town_fips_dp$data[[1]])

raw_fips <- merge(raw_clean, town_fips, by = "Town", all=T)

# Drop factors we don't care about/want/need
# We will be removing "place of delivery" from this when we get fixed data from DPH
raw_fips <- raw_fips[!(raw_fips$Column %in% c("Live Birth Order", "Infant's Sex", "Place of Delivery", "Plurality", "Adequacy of Prenatal Care", "Alcohol Use During Pregnancy")),]

setDT(raw_fips)
# Combine all racial/ethnic factors into one filter
raw_fips[raw_fips$Column %in% c("Mother's Race","Mother's Ethnicity"),Column:="Mother's Race/Ethnicity"]

raw_fips$GROUPDEX <- NULL
raw_fips$LABEL <- NULL
raw_fips$COLUMN <- NULL
raw_fips$GROUP <- NULL
raw_fips$TWNRES <- NULL
raw_fips$GEOG_level <- NULL
raw_fips$GEOG_ID <- NULL
raw_fips$BWu500 <- NULL
raw_fips$BW500 <- NULL
raw_fips$BW1000 <- NULL
raw_fips$BW1500 <- NULL
raw_fips$BW2500 <- NULL
raw_fips$BW3500 <- NULL
raw_fips$BW_UNK <- NULL

# reshape
raw_fips_long <- melt(
  raw_fips,
  id.vars=c(
    "Town",
    "FIPS",
    "Aggregation",
    "Year",
    "Column",
    "Level",
    "Births"
  ),
  variable.name="Variable",
  value.name="Value"
)

#add columns for weight, age, measure type vars
raw_fips_long[,`:=`("Birth Weight" = "All", "Gestational Age" = "All", "Measure Type" = "Number")]

#begin populating columns
p_vlbw <- raw_fips_long$Variable == "P_VLBW"
raw_fips_long[p_vlbw,]$"Birth Weight" <- "Very Low Birth Weight (under 1500 grams)"
raw_fips_long[p_vlbw,]$"Measure Type" <- "Percent"
remove(p_vlbw)

vlbw <- raw_fips_long$Variable == "VLBW"
raw_fips_long[vlbw,]$"Birth Weight" <- "Very Low Birth Weight (under 1500 grams)"
remove(vlbw)

p_lbw <- raw_fips_long$Variable == "P_LBW"
raw_fips_long[p_lbw,]$"Birth Weight" <- "Low Birth Weight (under 2500 grams)"
raw_fips_long[p_lbw,]$"Measure Type" <- "Percent"
remove(p_lbw)

lbw <- raw_fips_long$Variable == "LBW"
raw_fips_long[lbw,]$"Birth Weight" <- "Low Birth Weight (under 2500 grams)"
remove(lbw)

preterm <- raw_fips_long$Variable == "PreTerm"
raw_fips_long[preterm,]$"Gestational Age" <- "Less than 37 weeks"
remove(preterm)

term <- raw_fips_long$Variable == "Term"
raw_fips_long[term,]$"Gestational Age" <- "37 weeks or more"

p_term <- raw_fips_long[term,]
p_term[,`:=`(
  "Measure Type" = "Percent",
  Value = ifelse(Births > 0, round((Value/Births)*100, 2), 0)
)]
raw_fips_long <- rbind(raw_fips_long, p_term)
remove(p_term,term)

unkterm <- raw_fips_long$Variable == "UnkTerm"
raw_fips_long[unkterm,]$"Gestational Age" <- "Gestational age unknown"

p_unkterm <- raw_fips_long[unkterm,]
p_unkterm[,`:=`(
  "Measure Type" = "Percent",
  Value = ifelse(Births > 0, round((Value/Births)*100, 2), 0)
)]
raw_fips_long <- rbind(raw_fips_long, p_unkterm)
remove(p_unkterm, unkterm)

p_preterm <- raw_fips_long$Variable == "P_Preterm"
raw_fips_long[p_preterm,]$"Gestational Age" <- "Less than 37 weeks"
raw_fips_long[p_preterm,]$"Measure Type" <- "Percent"
remove(p_preterm)

#set variable name
raw_fips_long$Variable <- "Births"

#Apply Suppression
#Suppress values with MT = Number (anything less than 15)
Number <- raw_fips_long[raw_fips_long$`Measure Type` == "Number",]
Number$Value[which(Number$Value > 0 & Number$Value < 15)] <- -9999

#Suppress values with MT = Percent 
Percent <- raw_fips_long[raw_fips_long$`Measure Type` == "Percent",]
Percent <- Percent %>% 
  mutate(calc_num = (Value/100) * Births)

#if calc_num < 15, suppress the percent
Percent$Value[which(Percent$calc_num > 0 & Percent$calc_num < 15)] <- -9999
Percent$calc_num <- NULL

raw_fips_long <- rbind(Number, Percent)

raw_fips_long_list <- list(
  raw_fips_long[raw_fips_long$Column %in% c("Mother's Education", "Mother's Marital Status")], #Socioeconomic characteristics
  raw_fips_long[raw_fips_long$Column %in% c("Initiation of Prenatal Care", "Smoking During Pregnancy")], #Behaviour
  raw_fips_long[raw_fips_long$Column %in% c("Mother's Age", "Mother's Race/Ethnicity")]  #Demographic Characteristics
)
####################################################################################################################################################
fileNames <- c(
  "background",
  "behavior",
  "demographics"
)

for (i in 1:3) { #iterate through parts of this dataset.
  dataset <- raw_fips_long_list[[i]]
  # Convert labels into columns
  for(col in unique(dataset$Column)) {
    dataset[,
            eval(col):=ifelse(col == Column, Level, "All")
            ]
  }
  
  # Remove working cols
  dataset[,`:=`(
    Column = NULL,
    Level  = NULL,
    Births = NULL
  )]
  #remove newly created duplicates (all categories say "all")
  setkey(dataset)
  dataset <- unique(dataset)
  
  if ("Mother's Age" %in% names(dataset)) {
    #aggregate age groups (15 to 19 Years)
    agg.1519 <- dataset[dataset$"Mother's Age" %in% paste(15:19, "yrs", sep=" ")]
    dataset <- dataset[!(dataset$"Mother's Age" %in% paste(15:19, "yrs", sep=" "))]
    bynames <- copy(names(agg.1519))
    bynames <- bynames[!(bynames %in% c("Value", "Mother's Age"))]
    agg.1519 <- agg.1519[,sum(Value),by=bynames]
    agg.1519[,`:=`(
      Value = V1,
      V1 = NULL,
      "Mother's Age" = "15 to 19 years"
    )]
    dataset <- rbind(dataset, agg.1519)
    remove(bynames, agg.1519)
  }
  
  # Order dataset so that the larger aggregations are at the top
  # prevents the ckan datastore from mishandling the column type.
  setkey(dataset, NULL)
  dataset <- dataset[order(-Aggregation),]
  
  # set column order, this works for all 3 subsets
  setcolorder(dataset, c(1:4,7,8,10:ncol(dataset),9,5,6))

  # write table to files
  for(y in c(1,3,5)) {
    agg <- paste(y, "Year", sep="-")
    filename <- paste("maternal_characteristics_", fileNames[i], "_", agg, ".csv", sep = "")
    
    # Which data are we writing?
    toWrite <- subset(dataset, Aggregation == agg)
    # drop aggregation column for output
    toWrite[
      ,
      Aggregation := NULL
      ]
    
    write.table(
      toWrite,
      sep = ",",
      file.path(path_to_data, filename),
      row.names = F
    )
  }
}
