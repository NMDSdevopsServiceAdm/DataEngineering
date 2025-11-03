import polars as pl

from polars_utils import logger, utils
from projects._01_ingest.cqc_api.fargate.utils import locations_4_clean_utils as cUtils
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
from utils.cqc_local_authority_provider_ids import LocalAuthorityProviderIds

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

    cqc_lf = cUtils.clean_and_impute_registration_date(cqc_lf)

    # TODO - (1115) remove_specialist_colleges

    # TODO - (1116) save deregistered locations for reconciliation process
    # - filter to deregistered locations only in the most recent import date
    # - select cols req by reconciliation process
    #   (CQCLClean.cqc_location_import_date, CQCLClean.location_id, CQCLClean.registration_status, CQCLClean.deregistration_date)
    # - sink parquet to s3
    # - Create ticket to update reconciliation process after this (some steps won't be required now)

    cqc_reg_lf = cqc_lf.filter(
        pl.col(CQCLClean.registration_status) == RegistrationStatus.registered
    )

    cqc_reg_lf = cUtils.clean_provider_id_column(cqc_reg_lf)

    cqc_reg_lf = cUtils.impute_missing_values(
        cqc_reg_lf,
        [
            CQCLClean.provider_id,
            CQCLClean.services_offered,
            CQCLClean.specialisms_offered,
            CQCLClean.regulated_activities_offered,
            CQCLClean.relationships_types,
            CQCLClean.registered_manager_names,
        ],
    )

    cqc_reg_lf = cUtils.allocate_primary_service_type(cqc_reg_lf)

    cqc_reg_lf = cUtils.realign_carehome_column_with_primary_service(cqc_reg_lf)

    cqc_reg_lf = cqc_reg_lf.filter(
        pl.col(CQCLClean.provider_id).is_not_null(),
        pl.col(CQCLClean.regulated_activities_offered).is_not_null(),
    )

    cqc_reg_lf = cUtils.assign_cqc_sector(
        cqc_reg_lf, la_provider_ids=LocalAuthorityProviderIds.known_ids
    )

    cqc_reg_lf = cUtils.add_related_location_column(cqc_reg_lf)
    cqc_reg_lf = cqc_reg_lf.drop(CQCLClean.relationships_types)

    # Scan parquet to get ONS Postcode Directory data in LazyFrame format
    ons_lf = utils.scan_parquet(
        ons_postcode_directory_source,
        selected_columns=ons_cols_to_import,
    )
    logger.info("ONS Postcode Directory LazyFrame read in")
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
