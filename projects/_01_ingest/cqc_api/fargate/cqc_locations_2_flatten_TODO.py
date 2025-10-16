# TODO - scan parquet to get delta data (only select cols required)

# TODO - remove_records_from_locations_data

# TODO - remove locations who have never been a social care locations

# TODO - create_cleaned_registration_date_column
# TODO - column_to_date (imputed_registration_date)
# TODO - format_date_fields (both registration dates)

# TODO - column_to_date (cqc_location_import_date)

# TODO - clean_provider_id_column
# TODO - select_rows_with_non_null_value (provider_id)
# TODO - add_cqc_sector_column_to_cqc_locations_dataframe

# TODO - impute_historic_relationships

# TODO - impute_missing_struct_column (gac_service_types, regulated_activities, specialisms)

# TODO - remove_locations_that_never_had_regulated_activities

# TODO - extract_from_struct (services_offered, specialisms_offered)

# TODO - classify_specialisms (dementia, learning_disabilities, mental_health)

# TODO - allocate_primary_service_type
# TODO - realign_carehome_column_with_primary_service

# TODO - extract_registered_manager_names
# TODO - add_related_location_column
# TODO - run_postcode_matching (filter to relevant locations only)

# TODO - drop unrequired cols
# TODO - sink_parquet to store flattened data in s3
