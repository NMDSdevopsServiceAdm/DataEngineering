import polars as pl

import projects._01_ingest.cqc_api.fargate.utils.flatten_utils as FUtils
from polars_utils import logger, utils
from polars_utils.cleaning_utils import column_to_date
from schemas.cqc_locations_schema_polars import POLARS_LOCATION_SCHEMA
from utils.column_names.cleaned_data_files.cqc_location_cleaned import (
    CqcLocationCleanedColumns as CQCLClean,
)
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys

logger = logger.get_logger(__name__)

cqc_partition_keys = [Keys.year, Keys.month, Keys.day, Keys.import_date]

cqc_location_cols_to_import = [
    CQCLClean.location_id,
    CQCLClean.provider_id,
    CQCLClean.name,
    CQCLClean.postal_address_line1,
    CQCLClean.postal_code,
    CQCLClean.registration_status,
    CQCLClean.registration_date,
    CQCLClean.deregistration_date,
    CQCLClean.type,
    CQCLClean.relationships,
    CQCLClean.care_home,
    CQCLClean.number_of_beds,
    CQCLClean.dormancy,
    CQCLClean.gac_service_types,
    CQCLClean.regulated_activities,
    CQCLClean.specialisms,
    Keys.import_date,
    Keys.year,
    Keys.month,
    Keys.day,
]


def main(
    cqc_locations_api_delta_source: str,
    cqc_locations_flattened_destination: str,
) -> None:
    # Scan parquet to get CQC locations delta data in LazyFrame format
    cqc_lf = utils.scan_parquet(
        cqc_locations_api_delta_source,
        schema=POLARS_LOCATION_SCHEMA,
        selected_columns=cqc_location_cols_to_import,
    )
    logger.info("CQC Location LazyFrame read in")

    # TODO - remove_records_from_locations_data

    # TODO - remove locations who have never been a social care locations

    # TODO - create_cleaned_registration_date_column
    # TODO - column_to_date (imputed_registration_date)
    cqc_lf = column_to_date(cqc_lf, CQCLClean.registration_date)
    cqc_lf = FUtils.clean_and_impute_registration_date(cqc_lf)

    # TODO - format_date_fields (both registration dates)
    cqc_lf = column_to_date(cqc_lf, CQCLClean.deregistration_date)

    # TODO - column_to_date (cqc_location_import_date)
    cqc_lf = column_to_date(
        cqc_lf, Keys.import_date, CQCLClean.cqc_location_import_date
    )

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

    # TODO - drop unrequired cols

    # Store flattened data in s3
    utils.sink_to_parquet(
        cqc_lf,
        cqc_locations_flattened_destination,
        logger=logger,
        partition_cols=cqc_partition_keys,
        append=False,
    )


if __name__ == "__main__":
    logger.info("Running Flatten CQC Locations job")

    args = utils.get_args(
        (
            "--cqc_locations_api_delta_source",
            "S3 URI to read CQC locations raw API delta data from",
        ),
        (
            "--cqc_locations_flattened_destination",
            "S3 URI to save flattened CQC locations data to",
        ),
    )

    main(
        cqc_locations_api_delta_source=args.cqc_locations_api_delta_source,
        cqc_locations_flattened_destination=args.cqc_locations_flattened_destination,
    )

    logger.info("Finished Flatten CQC Locations job")
    logger.info("Finished Flatten CQC Locations job")
