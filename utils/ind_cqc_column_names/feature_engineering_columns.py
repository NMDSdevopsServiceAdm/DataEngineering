from dataclasses import dataclass

from utils.ind_cqc_column_names.prepare_locations_cleaned_columns import (
    PrepareLocationsCleanedColumns,
)

@dataclass
class FeatureEngineeringColumns:
    care_home = PrepareLocationsCleanedColumns.care_home
    features: str = "features"
    job_count = PrepareLocationsCleanedColumns.job_count
    location_id = PrepareLocationsCleanedColumns.location_id
    number_of_beds = PrepareLocationsCleanedColumns.number_of_beds
    ons_region = PrepareLocationsCleanedColumns.ons_region
    partition_0: str = "partition_0"
    people_directly_employed = PrepareLocationsCleanedColumns.people_directly_employed
    snapshot_date = PrepareLocationsCleanedColumns.snapshot_date
    snapshot_day = PrepareLocationsCleanedColumns.snapshot_day
    snapshot_month = PrepareLocationsCleanedColumns.snapshot_month
    snapshot_year = PrepareLocationsCleanedColumns.snapshot_year

