# TODO - scan_parquet to get full flattened data (will be the full file, not delta) (only select cols required)

# TODO - review order of tasks below

# TODO - remove_non_social_care_locations (assuming some have switched between SC and not)
# TODO - remove_specialist_colleges

# TODO - select_deregistered_locations_only (only those within 2 months of import date)
# TODO - sink_parquet to store deregistered cleaned data in s3 (only cols req by reconciliation) - Create ticket to do reconciliation process after this

# TODO - select_registered_locations_only

# TODO - join in ONS postcode data
# TODO - run_postcode_matching (filter to relevant locations only)

# TODO - sink_parquet to store registered cleaned data in s3
