###GEM data wrangling - Gas/Oil
library(dplyr)
library(tidyr)
library(stringr)
library(readxl)
library(countrycode) # For converting country names to ISO2 codes
library(stringi)

coal <-read_excel("/Users/2diigermany/Desktop/Work/GEM /Coal/Global-Coal-Plant-Tracker-July-2024.xlsx", sheet = "Units")

GEM <-read_excel("/Users/2diigermany/Desktop/Work/GEM /OG/Global-Oil-and-Gas-Plant-Tracker-GOGPT-January-2025.xlsx", sheet = "Gas & Oil Units")
GEM_belowthreshhold <-read_excel("/Users/2diigermany/Desktop/Work/GEM /OG/Global-Oil-and-Gas-Plant-Tracker-GOGPT-January-2025.xlsx", sheet = "sub-threshold units")

# Combine GEM and GEM_belowthresholds by stacking the rows
GEM <- rbind(GEM, GEM_belowthreshhold)


# Create the 'classification' column based solely on the 'Fuel' column
# Process the dataset
GEM <- GEM %>%
  mutate(
    # Convert the Fuel column to lowercase for case-insensitive matching
    fuel_lower = tolower(Fuel),
    # Find the position where "fossil gas" appears (returns NA if not found)
    pos_gas = str_locate(fuel_lower, "fossil gas")[, "start"],
    # Find the position where "fossil liquids" appears (returns NA if not found)
    pos_liquid = str_locate(fuel_lower, "fossil liquids")[, "start"],
    # Classify based on which substring(s) are present and their order
    classification = case_when(
      # Only fossil gas is present
      !is.na(pos_gas) & is.na(pos_liquid) ~ "Gas Power Plant",
      # Only fossil liquids is present
      is.na(pos_gas) & !is.na(pos_liquid) ~ "Oil Power Plant",
      # Both are present: if "fossil gas" comes first, classify as Gas Power Plant;
      # otherwise (if "fossil liquids" comes first), classify as Oil Power Plant.
      !is.na(pos_gas) & !is.na(pos_liquid) ~ if_else(pos_gas < pos_liquid, "Gas Power Plant", "Oil Power Plant"),
      # In any other case (if neither substring is found), mark as Not Sure
      TRUE ~ "Not Sure"
    )
  ) %>%
  # Remove the helper columns if not needed
  select(-fuel_lower, -pos_gas, -pos_liquid)


###Note, I am choosing now to ignore the conversion for now
# conversion is used to signal what retired coal plant for example will be replaced with a new gasplant
# now technically, this should be covered by owners, but I think the owners are not as well aligned
# as we would wish (same owner/different name) something to align the datasets later on

GEM_relevant <- GEM %>%
  select(
    `Country/Area`,
    `Plant name`,
    `Unit name`,
    `Capacity (MW)`,
    `Status`,
    `Start year`,
    `Retired year`,
    `Planned retire`,
    `Owner(s)`,
    Latitude,
    Longitude,
    Region,
    `GEM location ID`,
    `GEM unit ID`,
    `classification`
  )

# we filter out retired, mothballed and cancelled
GEM_relevant <- GEM_relevant[GEM_relevant$Status %in% c("construction", "operating", "announced", "pre-construction"), ]

# we filter out classification "not sure"
GEM_relevant <- GEM_relevant[GEM_relevant$classification %in% c("Gas Power Plant","Oil Power Plant" ), ]


# Filter rows based on Status and Start year
# Plants that are announced or under construction, but we have no indication on when they are finished are kicked out.
# We kick them out, because they are not useable for forecasting
GEM_relevant <- GEM_relevant[!(GEM_relevant$Status %in% c("announced", "construction", "pre-construction") &
                                 (GEM_relevant$`Start year` == "not found" | is.na(GEM_relevant$`Start year`))), ]


# Replace ">0" with NA in the entire dataset
GEM_relevant[GEM_relevant == ">0"] <- "unknown"


# Filter out plants where we do not know the capacity
GEM_relevant <- GEM_relevant[ !(
  GEM_relevant$`Capacity (MW)` %in% c("N/A", "unknown") |
    is.na(GEM_relevant$`Capacity (MW)`) |
    GEM_relevant$`Capacity (MW)` == 0
), ]

#renaming some columns
GEM_relevant <- GEM_relevant %>%
  rename(
    Owner = `Owner(s)`,
    `Planned retirement` = `Planned retire`
  )


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

# Remove rows where GEM unit is NA
GEM_cleaned <- GEM_cleaned %>%
  filter(!is.na(`GEM unit ID`))

# Remove rows where Planned retirement is before 2024
GEM_cleaned <- GEM_cleaned %>%
  filter(is.na(`Planned retirement`) | `Planned retirement` >= 2024)

##Creating Time Series

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

GEM_timeseries_cleaned <- GEM_timeseries %>%
  select(-c(
    `Planned retirement`, `Retired year`, `Start year`, `Status`, `Unit name`, `GEM unit ID`,
  ))

# Aggregate values based on GEM location ID, Owner, and Year
GEM_timeseries_aggregated <- GEM_timeseries_cleaned %>%
  group_by(`GEM location ID`, Owner, `Country/Area`, `Plant name`, Latitude, Longitude, Region, classification, year) %>%
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
    production_year = year
  ) %>%
  mutate(
    country_iso2 = countrycode(country_name, "country.name", "iso2c"),
    country_iso2 = ifelse(country_name == "Kosovo", "XK", country_iso2),
    workforce_size = NA,         # Add new column, set to NA
    workforce_source = NA,         # Add new column, set to NA
    sector = "Power",              # Add new column, set to "Power"
    technology = case_when(        # Set technology based on classification
      classification == "Gas Power Plant" ~ "GasCap",
      classification == "Oil Power Plant" ~ "OilCap",
      TRUE ~ "CoalCap"
    ),
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

#no capacity factors, would require to use speicfic fuel type or technology (which might be available)

write.csv(GEM_timeseries_aggregated,"/Users/2diigermany/Desktop/Work/GEM /Wrangled/GEM_OG.csv", row.names = F)

#' to note
#' Gas and Oil plants split up into gas and oul based on their "primary" fuel
#' we assume primary fuel is what is named first in the fuel source column of GEM


