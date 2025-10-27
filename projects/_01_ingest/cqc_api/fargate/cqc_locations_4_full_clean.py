import polars as pl

from polars_utils import logger, utils
from utils.column_names.cleaned_data_files.cqc_location_cleaned import (
    CqcLocationCleanedColumns as CQCLClean,
)
from utils.column_names.cleaned_data_files.ons_cleaned import (
    OnsCleanedColumns as ONSClean,
)
from utils.column_names.cleaned_data_files.ons_cleaned import (
    contemporary_geography_columns,
    current_geography_columns,
)
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys
from utils.column_values.categorical_column_values import (
    LocationType,
    RegistrationStatus,
)

logger = logger.get_logger(__name__)

cqc_partition_keys = [Keys.year, Keys.month, Keys.day, Keys.import_date]

ons_cols_to_import = [
    ONSClean.postcode,
    *contemporary_geography_columns,
    *current_geography_columns,
]


def main(
    cqc_locations_full_flattened_source: str,
    ons_postcode_directory_source: str,
    cqc_registered_locations_cleaned_destination: str,
    cqc_deregistered_locations_destination: str,
) -> None:
    # Scan parquet to get CQC locations full data in LazyFrame format
    cqc_lf = utils.scan_parquet(
        cqc_locations_full_flattened_source,
    )
    logger.info("Full Flattened CQC Location LazyFrame read in")

    cqc_lf = cqc_lf.filter(
        pl.col(CQCLClean.type) == LocationType.social_care_identifier
    )

    # TODO - (1115) remove_specialist_colleges

    # TODO - (1153) impute_historic_relationships

    # TODO - (1116) save deregistered locations for reconciliation process
    # - filter to deregistered locations only in the most recent import date
    # - select cols req by reconciliation process
    #   (CQCLClean.cqc_location_import_date, CQCLClean.location_id, CQCLClean.registration_status, CQCLClean.deregistration_date)
    # - sink parquet to s3
    # - Create ticket to update reconciliation process after this (some steps won't be required now)

    cqc_reg_lf = cqc_lf.filter(
        pl.col(CQCLClean.registration_status) == RegistrationStatus.registered
    )

    # TODO - (1120) clean provider_id and add cqc_sector

    # TODO - (1155) move fUtils.impute_missing_struct_columns from cqc_locations_2_flatten to utils.flatten_utils

    # TODO - (1118) remove_locations_that_never_had_regulated_activities

    # TODO - (1125) add_related_location_column

    # Scan parquet to get ONS Postcode Directory data in LazyFrame format
    ons_lf = utils.scan_parquet(
        ons_postcode_directory_source,
        selected_columns=ons_cols_to_import,
    )
    logger.info("CQC Location LazyFrame read in")
    # TODO - (1117) join in ONS postcode data / run_postcode_matching (filter to relevant locations only if haven't already)

    # Store cleaned registered data in s3
    utils.sink_to_parquet(
        cqc_reg_lf,
        cqc_registered_locations_cleaned_destination,
        logger=logger,
        partition_cols=cqc_partition_keys,
        append=False,
    )


if __name__ == "__main__":
    logger.info("Running Clean Full CQC Locations job")

    args = utils.get_args(
        (
            "--cqc_locations_full_flattened_source",
            "S3 URI to read CQC locations full flattened data from",
        ),
        (
            "--ons_postcode_directory_source",
            "S3 URI to read ONS Postcode Directory data from",
        ),
        (
            "--cqc_registered_locations_cleaned_destination",
            "S3 URI to save full cleaned CQC registered locations data to",
        ),
        (
            "--cqc_deregistered_locations_destination",
            "S3 URI to save full cleaned CQC deregistered locations data to",
        ),
    )

    main(
        cqc_locations_full_flattened_source=args.cqc_locations_full_flattened_source,
        ons_postcode_directory_source=args.ons_postcode_directory_source,
        cqc_registered_locations_cleaned_destination=args.cqc_registered_locations_cleaned_destination,
        cqc_deregistered_locations_destination=args.cqc_deregistered_locations_destination,
    )

    logger.info("Finished Clean Full CQC Locations job")
