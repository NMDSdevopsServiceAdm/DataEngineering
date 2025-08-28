CREATE EXTERNAL TABLE IF NOT EXISTS dataset_locations_api_cleaned(
 contemporary_ons_import_date date
, postalcode string
, cqc_location_import_date date
, providerid string
, cqc_provider_import_date date
, locationid string
, carehome string
, dormancy string
, gacservicetypes array<struct<name:string, description:string>>
, name string
, numberofbeds int
, registrationstatus string
, registrationdate date
, deregistrationdate date
, regulatedactivities array<struct<name:string, code:string, contacts:array<struct<personFamilyName:string, personGivenName:string, personRoles:array<string>, personTitle:string, col3:array<string>>>>>
, specialisms array<struct<name:string>>
, type string
, relationships array<struct<relatedLocationId:string, relatedLocationName:string, type:string, reason:string>>
, imputed_registration_date date
, time_registered bigint
, time_since_dormant bigint
, imputed_relationships array<struct<relatedLocationId:string, relatedLocationName:string, type:string, reason:string>>
, imputed_gacservicetypes array<struct<name:string, description:string>>
, imputed_regulatedactivities array<struct<name:string, code:string, contacts:array<struct<personFamilyName:string, personGivenName:string, personRoles:array<string>, personTitle:string, col3:array<string>>>>>
, imputed_specialisms array<struct<name:string>>
, services_offered array<string>
, specialisms_offered array<string>
, primary_service_type string
, registered_manager_names array<string>
, related_location string
, provider_name string
, cqc_sector string
, contemporary_cssr string
, contemporary_region string
, contemporary_sub_icb string
, contemporary_icb string
, contemporary_icb_region string
, contemporary_ccg string
, contemporary_lat string
, contemporary_long string
, contemporary_imd string
, contemporary_lsoa11 string
, contemporary_msoa11 string
, contemporary_ru11ind string
, contemporary_lsoa21 string
, contemporary_msoa21 string
, contemporary_pcon string
, current_ons_import_date date
, current_cssr string
, current_region string
, current_sub_icb string
, current_icb string
, current_icb_region string
, current_ccg string
, current_lat string
, current_long string
, current_imd string
, current_lsoa11 string
, current_msoa11 string
, current_ru11ind string
, current_lsoa21 string
, current_msoa21 string
, current_pcon string
, day string
, import_date string
)
PARTITIONED BY (year string, month string, last_updated string)
STORED AS PARQUET
LOCATION 's3://sfc-test-diff-datasets/domain=CQC/dataset=locations_api_cleaned/'


//MSCK REPAIR TABLE dataset_locations_api_cleaned;