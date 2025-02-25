library(dplyr)
library(countrycode)

#read wrangled data

coal<- read.csv(file="/Users/2diigermany/Desktop/Work/GEM /Wrangled/GEM_coal.csv")
og <- read.csv(file="/Users/2diigermany/Desktop/Work/GEM /Wrangled/GEM_OG.csv")
hydro <- read.csv(file="/Users/2diigermany/Desktop/Work/GEM /Wrangled/GEM_Hydro.csv")
solar <- read.csv(file="/Users/2diigermany/Desktop/Work/GEM /Wrangled/GEM_Solar.csv")
wind <- read.csv(file="/Users/2diigermany/Desktop/Work/GEM /Wrangled/GEM_Wind.csv")
nuclear <- read.csv(file="/Users/2diigermany/Desktop/Work/GEM /Wrangled/GEM_Nuclear.csv")
geo <- read.csv(file="/Users/2diigermany/Desktop/Work/GEM /Wrangled/GEM_geothermal.csv")
bio <- read.csv(file="/Users/2diigermany/Desktop/Work/GEM /Wrangled/GEM_Bioenergy.csv")

# Combine all datasets
data <- rbind(coal, og, hydro, solar, wind, nuclear, geo, bio)



# Extract unique company names
unique_companies <- unique(data$company_name)

# Generate unique IDs: "TFL" + 8-digit random numbers
set.seed(123)  # Ensures reproducibility
company_ids <- paste0("TFL", sprintf("%08d", sample(10000000:99999999, length(unique_companies), replace = FALSE)))

# Create a mapping dataframe
company_mapping <- data.frame(company_name = unique_companies, company_id = company_ids, stringsAsFactors = FALSE)

# Merge mapping with the original dataset to assign the new IDs
data <- data %>%
  left_join(company_mapping, by = "company_name") %>%
  mutate(company_id = ifelse(is.na(company_id.x), company_id.y, company_id.x)) %>%
  select(-company_id.x, -company_id.y)  # Remove old columns after replacement


# Ensure the column order remains unchanged
data <- data[, c("asset_id", "asset_name", "company_id", "company_name", "country_iso2",
                 "country_name", "region", "coordinates", "workforce_size", "workforce_source",
                 "sector", "technology", "capacity", "capacity_unit", "production_year",
                 "plant_age_years", "plant_age_rank", "capacity_factor", "emission_factor")]


##adding steel
steel <- read.csv(file="/Users/2diigermany/Desktop/Work/GEM /Wrangled/GEM_steel.csv")

# Step 1: Ensure 'steel' has unique company_name → company_id mapping (keep first match if duplicated)
steel_unique <- steel %>%
  group_by(company_name) %>%
  slice(1) %>%  # Keeps the first occurrence if duplicates exist
  ungroup()

# Step 2: Join data with steel to get matching company_id
data_updated <- data %>%
  left_join(select(steel_unique, company_name, company_id), by = "company_name") %>%
  mutate(company_id = ifelse(!is.na(company_id.y), company_id.y, company_id.x)) %>%
  select(-company_id.x, -company_id.y)  # Remove temp columns

data <- rbind(data_updated, steel)




##adding emission factors
CT <- read.csv(file="/Users/2diigermany/Desktop/Work/GEM /Emission Factors Data Climate Trace/electricity-generation_emissions_sources.csv", sep=";")

## emission factors from climate trace, average over the available data for the latest point of time for each country
## average over country average for globalto replace missing country
# type only oil, gas and coal for now, although more variation might be possible in the future
# Convert end_time to a Date-Time format
CT <- CT %>%
  mutate(end_time = as.POSIXct(end_time, format = "%d.%m.%y %H:%M", tz = "UTC"))

# Filter for end_time "31.12.24 00:00"
filtered_data <- CT %>%
  filter(end_time == as.POSIXct("31.12.24 00:00", format = "%d.%m.%y %H:%M", tz = "UTC"))

# Group by source_type and iso3_country, then calculate mean emissions_factor
mean_emission_factors <- filtered_data %>%
  group_by(source_type, iso3_country) %>%
  summarise(mean_emissions_factor = mean(emissions_factor, na.rm = TRUE)) %>%
  ungroup()

# Filter for source_type values: "oil", "gas", "coal"
filtered_mean_emission_factors <- mean_emission_factors %>%
  filter(source_type %in% c("oil", "gas", "coal"))

# Calculate global mean for each source_type
global_means <- filtered_mean_emission_factors %>%
  group_by(source_type) %>%
  summarise(iso3_country = "Global",
            mean_emissions_factor = mean(mean_emissions_factor, na.rm = TRUE)) %>%
  ungroup()

# Append global means to the original filtered dataset
final_mean_emission_factors <- bind_rows(filtered_mean_emission_factors, global_means)

# Emission Factor is in t of CO2/MWH

# Step 1: Add country_iso2 to final_mean_emission_factors
final_mean_emission_factors <- final_mean_emission_factors %>%
  mutate(country_iso2 = ifelse(iso3_country == "Global", "Global",
                               countrycode(iso3_country, "iso3c", "iso2c")))

# Step 2: Rename source_type to technology
final_mean_emission_factors <- final_mean_emission_factors %>%
  mutate(technology = case_when(
    source_type == "coal" ~ "CoalCap",
    source_type == "oil"  ~ "OilCap",
    source_type == "gas"  ~ "GasCap",
    TRUE ~ source_type  # Keep other values unchanged (shouldn't be any)
  )) %>%
  select(-source_type)  # Drop the old source_type column

# Step 3: Merge data with emission factors
data <- data %>%
  left_join(final_mean_emission_factors, by = c("technology", "country_iso2")) %>%
  mutate(emission_factor = ifelse(is.na(emission_factor), mean_emissions_factor, emission_factor)) %>%
  select(-mean_emissions_factor, -iso3_country)  # Drop unnecessary columns

# Step 4: Replace missing emission factors with "Global" values
# Ensure "Global" emission factors are correctly named before joining
global_emissions <- final_mean_emission_factors %>%
  filter(country_iso2 == "Global") %>%
  select(technology, mean_emissions_factor) %>%
  rename(global_emissions_factor = mean_emissions_factor)

# Join "Global" emission factors and replace NAs
data <- data %>%
  left_join(global_emissions, by = "technology") %>%
  mutate(emission_factor = ifelse(is.na(emission_factor), global_emissions_factor, emission_factor)) %>%
  select(-global_emissions_factor)  # Remove extra column
# Step 5: Set emission_factor to 0 for other technologies
data <- data %>%
  mutate(emission_factor = ifelse(technology %in% c("CoalCap", "OilCap", "GasCap"), emission_factor, 0))




write.csv(data,"/Users/2diigermany/Desktop/Work/GEM /Wrangled/GEM_data_total.csv", row.names = F )


# Extract only asset_id and coordinates, then remove duplicates
data_unique <- data %>%
  select(asset_id, coordinates) %>%
  distinct()

write.csv(data,"/Users/2diigermany/Desktop/Work/GEM /Wrangled/GEM_uniqueassets_locations.csv", row.names = F )


