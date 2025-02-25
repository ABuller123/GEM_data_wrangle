###GEM data wrangling - Coal
library(dplyr)
library(tidyr)
library(stringr)
library(readxl)
library(countrycode) # For converting country names to ISO2 codes
library(stringi)


GEM <-read_excel("/Users/2diigermany/Desktop/Work/GEM /Coal/Global-Coal-Plant-Tracker-July-2024.xlsx", sheet = "Units")
steel <- read.csv(file="/Users/2diigermany/Library/CloudStorage/OneDrive-2°investinginitiative/Desktop/Task/Scenarios/New Sectors/Steel/Steel third party data/GEM_wrangled/assets.csv")


library(dplyr)

# Selecting only the required columns
GEM_relevant <- GEM %>%
  select(
    `GEM unit/phase ID`,
    `GEM location ID`,
    `Country/Area`,
    `Plant name`,
    `Unit name`,
    `Owner`,
    `Capacity (MW)`,
    `Status`,
    `Start year`,
    `Retired year`,
    `Planned retirement`,
    `Combustion technology`,
    `Coal type`,
    `Latitude`,
    `Longitude`,
    `Remaining plant lifetime (years)`,
    `Plant age (years)`,
    `Region`,

  )

# we filter out retired, mothballed and cancelled
GEM_relevant <- GEM_relevant[GEM_relevant$Status %in% c("construction", "operating", "announced", "pre-permit", "permitted"), ]

# Filter rows based on Status and Start year
# Plants that are announced or under construction, but we have no indication on when they are finished are kicked out.
# We kick them out, because they are not useable for forecasting
GEM_relevant <- GEM_relevant[!(GEM_relevant$Status %in% c("announced", "construction","pre-permit", "permitted") &
                                 GEM_relevant$`Start year` == "unknown"), ]

# Replace ">0" with NA in the entire dataset
GEM_relevant[GEM_relevant == ">0"] <- "unknown"


# Filter out plants where we do not know the capacity
GEM_relevant <- GEM_relevant[!(GEM_relevant$`Capacity (MW)` %in% c("N/A", "unknown")), ]

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


# Next steps
# we have the asset ID and location ID, an asset ID is unique to the part of the plant
#
# proposal: We create the timelines for each asset ID, then sum up on the Location ID
# bigger propblem is that we dont have the parent id. , would need to make one up
# Also solar, wind nuclear dont have parent column, they have owner column but no ownership %

# Hydro has it with %
# Bioenergy has it with %

#how to deal with a plant that has multiple units, and one unit is owned partly by another company
#' option 1: Create the entire asset capacity pathway, make each company hold respective share of total capacity
#' option 2: sepearate the asset pathway beforehand, make the forecast, then aggregate on plant location and owner
#' 1 is more easy, 2 seems more correct
#'
#' One more thing is the remaining plant lifetime, this corresponds to the planned retirement column, but
#' not always is a planned retirement given. so what do we assume here? that the plant is just rolled over?
#' I feel this is critical information, but not sure how to take this into account yet


## creating the timeline
# each plant unit is here as a standalone row, not like in steel, where it was related to main plants

# Function to split ownership and distribute capacity
# each unit is split up into its different owners, and the capacity is alocated based on ownership shares
split_ownership <- function(df) {
  df %>%
    # Separate multiple owners into different rows
    separate_rows(Owner, sep = ";\\s*") %>%
    # Extract the company name and percentage
    mutate(
      Company = str_extract(Owner, "^[^\\[]+"),  # Extract company name before [
      Ownership = as.numeric(str_extract(Owner, "\\d+(?=%)")) / 100,  # Extract percentage and convert to decimal
      Capacity_allocated = `Capacity (MW)` * Ownership  # Adjusted capacity
    ) %>%
    select(-Owner, -Ownership, -"Capacity (MW)") %>%  # Remove unneeded columns
    rename(Owner = Company)  # Rename for clarity
}

# Apply function to the dataset
GEM_cleaned <- split_ownership(GEM_relevant)

# Remove rows where GEM unit/phase ID is NA
GEM_cleaned <- GEM_cleaned %>%
  filter(!is.na(`GEM unit/phase ID`))


# Remove rows where Planned retirement is before 2024
GEM_cleaned <- GEM_cleaned %>%
  filter(is.na(`Planned retirement`) | `Planned retirement` >= 2024)



##
# Define the time range
years <- 2023:2050

# Function to create a time series for each row
expand_time_series <- function(df) {
  df %>%
    # Expand dataset by adding a row for each year in the time series
    tidyr::crossing(year = years) %>%
    # Assign capacity values based on start year and planned retirement
    mutate(
      value = case_when(
        year < `Start year` ~ 0,  # Before start year, capacity is 0
        !is.na(`Planned retirement`) & year >= `Planned retirement` & `Planned retirement` <= 2050 ~ 0,  # After planned retirement, if within range
        TRUE ~ Capacity_allocated  # Otherwise, capacity stays constant
      )
    )
}

# Apply function to expand the dataset
GEM_timeseries <- expand_time_series(GEM_cleaned)



##Aggregation of capacities per plant and owner
# Remove specified columns
GEM_timeseries_cleaned <- GEM_timeseries %>%
  select(-c(
    `Remaining plant lifetime (years)`, `Coal type`, `Combustion technology`,
    `Planned retirement`, `Retired year`, `Start year`, `Status`, `Unit name`, `GEM unit/phase ID`
  ))

# Aggregate values based on GEM location ID, Owner, and Year
GEM_timeseries_aggregated <- GEM_timeseries_cleaned %>%
  group_by(`GEM location ID`, Owner, `Country/Area`, `Plant name`,`Plant age (years)`, Latitude, Longitude, Region, year) %>%
  summarise(
    Capacity_allocated = sum(Capacity_allocated, na.rm = TRUE),
    value = sum(value, na.rm = TRUE),
    .groups = "drop"
  )


##creating TFL Owner IDs
# add this as a last step

GEM_timeseries_aggregated$company_id <- NA

##merging lat and long
# Create a new column combining Latitude and Longitude
GEM_timeseries_aggregated <- GEM_timeseries_aggregated %>%
  mutate(coordinates = paste0(Latitude, ", ", Longitude)) %>%
  select(-Latitude, -Longitude)  # Remove the original columns

# Rename and modify columns
GEM_timeseries_aggregated <- GEM_timeseries_aggregated %>%
  rename(
    asset_id = `GEM location ID`,
    asset_name = `Plant name`,
    company_id = company_id,
    company_name = Owner,
    country_name = `Country/Area`,
    region = Region,
    coordinates = coordinates,
    capacity = value,
    production_year = year,
    plant_age_years = `Plant age (years)`
  ) %>%
  mutate(
    country_iso2 = countrycode(country_name, "country.name", "iso2c"),  # Convert country names to ISO2 codes
    country_iso2 = ifelse(country_name == "Kosovo", "XK", country_iso2),  # Manually handle Kosovo
    workforce_size = NA,  # Add new column, set to NA
    workforce_source = NA,  # Add new column, set to NA
    sector = "Power",  # Add new column, set to "Power"
    technology = "CoalCap",  # Add new column, set to "CoalCap"
    capacity_unit = "MW",  # Add new column, set to "MW"
    plant_age_rank = NA,  # Add new column, set to NA
    capacity_factor = NA,  # Add new column, set to NA
    emission_factor = NA  # Add new column, set to NA
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


###Final dataset see above GEM_timeseries_aggregated
#' I could add the emissions factor, that would be a benefit here, check that again with AI
#'
write.csv(GEM_timeseries_aggregated, "/Users/2diigermany/Desktop/Work/GEM /Wrangled/GEM_coal.csv", row.names = F)


##might want to add the capacity factors

##adding emission factor
