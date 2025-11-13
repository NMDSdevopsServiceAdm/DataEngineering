import polars as pl

from polars_utils import raw_data_adjustments, utils
from polars_utils.cleaning_utils import column_to_date
from projects._01_ingest.cqc_api.fargate.utils import cleaning_utils as cUtils
from projects._01_ingest.cqc_api.fargate.utils import postcode_matcher as pmUtils
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
    Specialisms,
)
from utils.cqc_local_authority_provider_ids import LocalAuthorityProviderIds

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
    cqc_full_snapshot_destination: str,
    manual_postcode_corrections_source: str,
) -> None:
    # Scan parquet to get CQC locations full data in LazyFrame format
    cqc_lf = utils.scan_parquet(
        cqc_locations_full_flattened_source,
    )
    print("Full Flattened CQC Location LazyFrame read in")

    cqc_lf = cqc_lf.filter(raw_data_adjustments.is_valid_location())

    cqc_lf = cqc_lf.filter(
        pl.col(CQCLClean.type) == LocationType.social_care_identifier
    )

    cqc_lf = column_to_date(
        cqc_lf, Keys.import_date, CQCLClean.cqc_location_import_date
    )

    cUtils.save_latest_full_snapshot(cqc_lf, cqc_full_snapshot_destination)

    cqc_lf = cUtils.clean_and_impute_registration_date(cqc_lf)

    cqc_lf = cUtils.clean_provider_id_column(cqc_lf)

    cqc_lf = cUtils.impute_missing_values(
        cqc_lf,
        [
            CQCLClean.provider_id,
            CQCLClean.services_offered,
            CQCLClean.specialisms_offered,
            CQCLClean.regulated_activities_offered,
            CQCLClean.relationships_types,
            CQCLClean.registered_manager_names,
        ],
    )

    cqc_reg_lf = cqc_lf.filter(
        pl.col(CQCLClean.registration_status) == RegistrationStatus.registered
    )

    cqc_reg_lf = cUtils.allocate_primary_service_type(cqc_reg_lf)

    cqc_reg_lf = cUtils.allocate_primary_service_type_second_level(cqc_reg_lf)

    cqc_reg_lf = cUtils.realign_carehome_column_with_primary_service(cqc_reg_lf)

    cqc_reg_lf = cqc_reg_lf.filter(
        pl.col(CQCLClean.provider_id).is_not_null(),
        pl.col(CQCLClean.regulated_activities_offered).is_not_null(),
    )

    cqc_reg_lf = cUtils.remove_specialist_colleges(cqc_reg_lf)

    cqc_reg_lf = cUtils.assign_cqc_sector(
        cqc_reg_lf, la_provider_ids=LocalAuthorityProviderIds.known_ids
    )

    cqc_reg_lf = cUtils.add_related_location_column(cqc_reg_lf)
    cqc_reg_lf = cqc_reg_lf.drop(CQCLClean.relationships_types)

    list_of_specialisms = [
        Specialisms.dementia,
        Specialisms.learning_disabilities,
        Specialisms.mental_health,
    ]
    cqc_reg_lf = cUtils.classify_specialisms(cqc_reg_lf, list_of_specialisms)

    ons_lf = utils.scan_parquet(
        ons_postcode_directory_source,
        selected_columns=ons_cols_to_import,
    )
    print("ONS Postcode Directory LazyFrame read in")

    cqc_reg_lf = pmUtils.run_postcode_matching(
        cqc_reg_lf,
        ons_lf,
        manual_postcode_corrections_source,
    )

    utils.sink_to_parquet(
        cqc_reg_lf,
        cqc_registered_locations_cleaned_destination,
        partition_cols=cqc_partition_keys,
        append=False,
    )


if __name__ == "__main__":
    print("Running Clean Full CQC Locations job")

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
            "--cqc_full_snapshot_destination",
            "S3 URI to save the latest full snapshot of CQC registered and deregistered locations",
        ),
        (
            "--manual_postcode_corrections_source",
            "Source s3 location for incorrect postcode csv dataset",
        ),
    )

    main(
        cqc_locations_full_flattened_source=args.cqc_locations_full_flattened_source,
        ons_postcode_directory_source=args.ons_postcode_directory_source,
        cqc_registered_locations_cleaned_destination=args.cqc_registered_locations_cleaned_destination,
        cqc_full_snapshot_destination=args.cqc_full_snapshot_destination,
        manual_postcode_corrections_source=args.manual_postcode_corrections_source,
    )

    print("Finished Clean Full CQC Locations job")
