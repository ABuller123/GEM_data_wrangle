###GEM data wrangling - hydro
library(dplyr)
library(tidyr)
library(stringr)
library(readxl)
library(countrycode) # For converting country names to ISO2 codes
library(stringi)

##Hydro

GEM <-read_excel("/Users/2diigermany/Desktop/Work/GEM /Power/Global-Hydropower-Tracker-April-2024.xlsx", sheet = "Data")

GEM_belowthreshold <-read_excel("/Users/2diigermany/Desktop/Work/GEM /Power/Global-Hydropower-Tracker-April-2024.xlsx", sheet = "Below Threshold")

# Combine GEM and GEM_belowthresholds by stacking the rows
GEM <- rbind(GEM, GEM_belowthreshold)


##one wrangling option would be to seperate the country into different rows, given
##that some plants are operating in two countries. This would allow for non unique Location IDs
##I propose to ignor this for now, given that this is only relevant for around 50 plants
# Or I create a sepearte row and go from there: adjust the location ID
GEM_national <- GEM[GEM$Binational=="No",]
GEM_binational <- GEM[GEM$Binational=="Yes",]


library(dplyr)

# (1) Create a copy for the Country 1 “side” (unchanged)
df_country1 <- GEM_binational %>%
  mutate(country_group = 1)  # helper column (will be dropped later)

# (2) Create a copy for the Country 2 “side.”
# Here we:
#   - Append "_2" to GEM location and unit IDs.
#   - Replace the “country‑1” fields with the corresponding “country‑2” fields.
df_country2 <- GEM_binational %>%
  mutate(country_group = 2,
         `GEM location ID` = paste0(`GEM location ID`, "_2"),
         `GEM unit ID`     = paste0(`GEM unit ID`, "_2"),
         # Now overwrite the country-specific columns
         `Country 1` = `Country 2`,
         `Country 1 Capacity (MW)` = `Country 2 Capacity (MW)`,
         `City 1`          = `City 2`,
         `Local Area 1`    = `Local Area 2`,
         `Major Area 1`    = `Major Area 2`,
         `State/Province 1`= `State/Province 2`,
         `Subregion 1`     = `Subregion 2`,
         `Region 1`        = `Region 2`
  )

# (3) Bind the two copies together. (The final data frame will have all the original columns.)
final_dataset <- bind_rows(df_country1, df_country2) %>%
  select(-country_group)  # remove the helper column if you wish

# Now, final_dataset has twice as many rows as GEM_binational.
# For rows coming from country2, the values originally stored in the “... 2” columns
# have been copied into the corresponding “... 1” columns, and GEM location/unit IDs have been modified.

# Step 4: Empty the Country 2 columns (set them to NA).
final_dataset <- final_dataset %>%
  mutate(
    `Country 2` = NA,
    `Country 2 Capacity (MW)` = NA,
    `City 2` = NA,
    `Local Area 2` = NA,
    `Major Area 2` = NA,
    `State/Province 2` = NA,
    `Subregion 2` = NA,
    `Region 2` = NA
  )

#merge binational and national back together
GEM <- rbind(GEM_national, final_dataset)

#select relevant columns
GEM_relevant <- GEM %>%
  select(
    `Country 1`,
    `Project Name`,
    `Country 1 Capacity (MW)`,
    `Status`,
    `Start Year`,
    `Retired Year`,
    `Owner`,
    `Technology Type`,
    `Latitude`,
    `Longitude`,
    `Region 1`,
    `GEM location ID`,
    `GEM unit ID`
  )

# we filter out retired, mothballed and cancelled, shelved
GEM_relevant <- GEM_relevant[GEM_relevant$Status %in% c("construction", "operating", "announced", "pre-construction"), ]


# We have several hundret missing start years for announced and construction plants
# for now we set these start years to 2030
# for operating plants missing start years are set to 2024

GEM_relevant <- GEM_relevant %>%
  mutate(`Start Year` = case_when(
    Status %in% c("announced", "construction", "pre-construction") & (is.na(`Start Year`) | `Start Year` == "not found") ~ 2030,
    Status == "operating" & (is.na(`Start Year`) | `Start Year` == "not found") ~ 2024,
    TRUE ~ `Start Year`
  ))

# Replace ">0" with NA in the entire dataset
GEM_relevant[GEM_relevant == ">0"] <- "unknown"

#renaming some columns
GEM_relevant <- GEM_relevant %>%
  rename(
    `Capacity (MW)` = `Country 1 Capacity (MW)`,
    Country = `Country 1`,
    Region = `Region 1`
  )


# Filter out plants where we do not know the capacity
GEM_relevant <- GEM_relevant[ !(
  GEM_relevant$`Capacity (MW)` %in% c("N/A", "unknown") |
    is.na(GEM_relevant$`Capacity (MW)`) |
    GEM_relevant$`Capacity (MW)` == 0
), ]

# Function to split ownership and distribute capacity
# each unit is split up into its different owners, and the capacity is alocated based on ownership shares

# Remove rows where Owner is NA and clean the Owner column
## again multiple rows lost, but really no idea on how to proceed without owner
GEM_relevant <- GEM_relevant %>%
  filter(!is.na(Owner))

###Several units from plants have slightly different coordinates
## we take for these cases the average coordinate and match that to the unique location id
# Ensure Latitude and Longitude are numeric
GEM_relevant <- GEM_relevant %>%
  mutate(Latitude = as.numeric(Latitude), Longitude = as.numeric(Longitude))

# Process the data
GEM_summary <- GEM_relevant %>%
  group_by(`GEM location ID`) %>%
  summarise(
    unique_latlong = n_distinct(Latitude, Longitude),  # Count unique lat-long pairs
    Latitude = ifelse(unique_latlong > 1, mean(Latitude), first(Latitude)),  # Mean if multiple, keep as is otherwise
    Longitude = ifelse(unique_latlong > 1, mean(Longitude), first(Longitude))  # Same for Longitude
  ) %>%
  select(-unique_latlong)  # Drop helper column


#Replace Latitude and Longitude in GEM_relevant with those from GEM_summary
GEM_relevant <- GEM_relevant %>%
  select(-Latitude, -Longitude) %>%  # Remove old Latitude & Longitude
  left_join(GEM_summary, by = "GEM location ID")  # Join new values


split_ownership <- function(df) {
  df %>%
    # Add an identifier to keep track of each original row
    mutate(row_id = row_number()) %>%
    # Split the Owner column into multiple rows if there is more than one owner (separated by ";")
    separate_rows(Owner, sep = ";\\s*") %>%
    # Extract the company name (everything before a "[" if present) and trim any whitespace
    mutate(
      Company = str_trim(str_extract(Owner, "^[^\\[]+")),
      # Extract the provided percentage (if any) using a lookbehind/lookahead regex.
      ProvidedOwnership = str_extract(Owner, "(?<=\\[)\\d+(?=%\\])"),
      # Convert the provided percentage into a decimal; will be NA if not provided.
      Ownership = as.numeric(ProvidedOwnership) / 100
    ) %>%
    # Group by the original row so we know how many owners each project had
    group_by(row_id) %>%
    mutate(
      # For rows where no percentage was provided (NA), assign an equal share
      Ownership = ifelse(is.na(Ownership), 1 / n(), Ownership),
      # Prepare a display string for the ownership percentage.
      # If a percentage was provided, use that; otherwise, use the computed share.
      DisplayOwnership = ifelse(is.na(ProvidedOwnership),
                                paste0(round(Ownership * 100, 2), "%"),
                                paste0(ProvidedOwnership, "%")),
      # If no percentage was originally given, append the computed percentage in brackets
      Owner = ifelse(is.na(ProvidedOwnership),
                     paste0(Company, " [", DisplayOwnership, "]"),
                     Owner),
      # Optionally, compute the allocated capacity based on the ownership share.
      Capacity_allocated = `Capacity (MW)` * Ownership
    ) %>%
    ungroup() %>%
    # Remove temporary columns used for computation
    select(-row_id, -Company, -ProvidedOwnership, -DisplayOwnership)
}

# Example usage:
# Assuming GEM_relevant is your dataset, run:
GEM_relevant_split <- split_ownership(GEM_relevant)


# Clean the Owner column to remove percentage information in brackets, with or without a preceding space
GEM_cleaned <- GEM_relevant_split %>%
  mutate(Owner = str_remove(Owner, " ?\\[[0-9]+(\\.[0-9]+)?%\\]"))


# Remove rows where GEM unit is NA
GEM_cleaned <- GEM_cleaned %>%
  filter(!is.na(`GEM unit ID`))


##Creating Time Series

# Define the time range
years <- 2023:2050

##adding a column to help with the calcultion
# Add a new column 'Planned retirement' with NA for all rows
GEM_cleaned$`Planned retirement` <- NA

# Function to create a time series for each row
expand_time_series <- function(df) {
  df %>%
    # Expand dataset by adding a row for each year in the time series
    tidyr::crossing(year = years) %>%
    # Assign capacity values based on start year and planned retirement
    mutate(
      value = case_when(
        year < `Start Year` ~ 0,  # Before start year, capacity is 0
        !is.na(`Planned retirement`) & year >= `Planned retirement` & `Planned retirement` <= 2050 ~ 0,  # After planned retirement, if within range
        TRUE ~ Capacity_allocated  # Otherwise, capacity stays constant
      )
    )
}

# Apply function to expand the dataset
GEM_timeseries <- expand_time_series(GEM_cleaned)

#remove some rows
GEM_timeseries_cleaned <- GEM_timeseries %>%
  select(-c(
    `Planned retirement`, `Retired Year`, `Start Year`, `Status`, `GEM unit ID`,
  ))

# Aggregate values based on GEM location ID, Owner, and Year
GEM_timeseries_aggregated <- GEM_timeseries_cleaned %>%
  group_by(`GEM location ID`, Owner, `Country`, `Project Name`, Latitude, Longitude, Region,`Technology Type`, year) %>%
  summarise(
    Capacity_allocated = sum(Capacity_allocated, na.rm = TRUE),
    value = sum(value, na.rm = TRUE),
    .groups = "drop"
  )


##creating TFL Owner IDs
# add this as a last step

GEM_timeseries_aggregated$company_id <- NA

# Create a new column combining Latitude and Longitude
GEM_timeseries_aggregated <- GEM_timeseries_aggregated %>%
  mutate(coordinates = paste0(Latitude, ", ", Longitude)) %>%
  select(-Latitude, -Longitude)  # Remove the original columns


#final wrangle
GEM_timeseries_aggregated <- GEM_timeseries_aggregated %>%
  rename(
    asset_id = `GEM location ID`,
    asset_name = `Project Name`,
    company_id = company_id,
    company_name = Owner,
    country_name = `Country`,
    region = Region,
    coordinates = coordinates,
    capacity = value,
    production_year = year
  ) %>%
  mutate(
    country_iso2 = countrycode(country_name, "country.name", "iso2c"),
    country_iso2 = ifelse(country_name == "Kosovo", "XK", country_iso2),
    workforce_size = NA,         # Add new column, set to NA
    workforce_source = NA,         # Add new column, set to NA
    sector = "Power",              # Add new column, set to "Power"
    technology = "HydroCap",
    capacity_unit = "MW",          # Add new column, set to "MW"
    plant_age_rank = NA,           # Add new column, set to NA
    capacity_factor = NA,          # Add new column, set to NA
    plant_age_years = NA,          # Add new column, set to NA
    emission_factor = NA           # Add new column, set to NA
  )

# Define the desired column order
desired_order <- c(
  "asset_id", "asset_name", "company_id", "company_name", "country_iso2",
  "country_name", "region", "coordinates", "workforce_size", "workforce_source",
  "sector", "technology", "capacity", "capacity_unit", "production_year",
  "plant_age_years", "plant_age_rank", "capacity_factor", "emission_factor"
)

# Reorder columns
GEM_timeseries_aggregated <- GEM_timeseries_aggregated %>%
  select(all_of(desired_order))

#write results
write.csv(GEM_timeseries_aggregated,"/Users/2diigermany/Desktop/Work/GEM /Wrangled/GEM_Hydro.csv", row.names = F)

#' to note
#' id system put at NA for now, need to add ids at the finals tage
#' hydro start year for announced/construction set to 2030 if missing
#' Ownership shares are assumed to be equally weighted if missing, (did not miss for all)
#' We added Gem_LocationID for assets that are split up in two countries, capacity is split up for each country, but ownership
#' stays the same
#'


