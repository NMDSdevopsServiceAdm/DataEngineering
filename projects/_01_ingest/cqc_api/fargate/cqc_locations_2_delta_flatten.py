from polars_utils import logger, raw_data_adjustments, utils
from polars_utils.cleaning_utils import column_to_date
from projects._01_ingest.cqc_api.fargate.utils import (
    locations_2_delta_flatten_utils as fUtils,
)
from projects._01_ingest.cqc_api.fargate.utils.extract_registered_manager_names import (
    extract_registered_manager_names,
)
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
    CQCLClean.current_ratings,
    CQCLClean.historic_ratings,
    CQCLClean.assessment,
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

    cqc_lf = column_to_date(cqc_lf, CQCLClean.registration_date)
    cqc_lf = column_to_date(cqc_lf, CQCLClean.deregistration_date)
    cqc_lf = column_to_date(
        cqc_lf, Keys.import_date, CQCLClean.cqc_location_import_date
    )

    # TODO - create_cleaned_registration_date_column

    fields_to_flatten = [
        (CQCL.gac_service_types, CQCL.description, CQCLClean.services_offered),
        (CQCL.specialisms, CQCL.name, CQCLClean.specialisms_offered),
        (CQCL.regulated_activities, CQCL.name, CQCLClean.regulated_activities_offered),
        (CQCL.relationships, CQCL.type, CQCLClean.relationships_types),
    ]
    cqc_lf = fUtils.flatten_struct_fields(cqc_lf, fields_to_flatten)

    # TODO - (1128) classify_specialisms (dementia, learning_disabilities, mental_health)
    # Move this into cqc_locations_4_full_clean. After imputation happens.

    cqc_lf = extract_registered_manager_names(cqc_lf)

    cqc_lf = cqc_lf.drop(
        CQCL.gac_service_types,
        CQCL.specialisms,
        CQCL.regulated_activities,
        CQCL.relationships,
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
