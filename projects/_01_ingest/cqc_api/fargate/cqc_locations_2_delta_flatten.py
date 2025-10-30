import polars as pl

from polars_utils import logger, raw_data_adjustments, utils
from projects._01_ingest.cqc_api.fargate.utils import flatten_utils as fUtils
from schemas.cqc_locations_schema_polars import POLARS_LOCATION_SCHEMA
from utils.column_names.cleaned_data_files.cqc_location_cleaned import (
    CqcLocationCleanedColumns as CQCLClean,
)
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys
from utils.column_names.raw_data_files.cqc_location_api_columns import (
    NewCqcLocationApiColumns as CQCL,
)

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

    cqc_lf = cqc_lf.filter(raw_data_adjustments.is_valid_location())

    # TODO - create_cleaned_registration_date_column
    # TODO - column_to_date (imputed_registration_date)
    # TODO - format_date_fields (both registration dates)

    # TODO - column_to_date (cqc_location_import_date)

    fields_to_flatten = [
        (CQCL.gac_service_types, CQCL.description, CQCLClean.services_offered),
        (CQCL.specialisms, CQCL.name, CQCLClean.specialisms_offered),
        (CQCL.regulated_activities, CQCL.name, CQCLClean.regulated_activities_offered),
    ]
    cqc_lf = fUtils.flatten_struct_fields(cqc_lf, fields_to_flatten)

    # TODO - (1128) classify_specialisms (dementia, learning_disabilities, mental_health)

    # TODO - (1127) allocate_primary_service_type
    # TODO - (1127) realign_carehome_column_with_primary_service

    # TODO - (1129) extract_registered_manager_names

    cqc_lf = cqc_lf.drop(
        CQCL.gac_service_types, CQCL.specialisms, CQCL.regulated_activities
    )

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
