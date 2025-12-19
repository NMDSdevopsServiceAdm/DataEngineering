from polars_utils import utils
from projects._03_independent_cqc._01_merge.fargate.utils.merge_utils import (
    join_data_into_cqc_lf,
)
from utils.column_names.capacity_tracker_columns import (
    CapacityTrackerCareHomeCleanColumns as CTCHClean,
)
from utils.column_names.capacity_tracker_columns import (
    CapacityTrackerNonResCleanColumns as CTNRClean,
)
from utils.column_names.cleaned_data_files.ascwds_workplace_cleaned import (
    AscwdsWorkplaceCleanedColumns as AWPClean,
)
from utils.column_names.cleaned_data_files.cqc_location_cleaned import (
    CqcLocationCleanedColumns as CQCLClean,
)
from utils.column_names.cleaned_data_files.cqc_pir_cleaned import (
    CqcPIRCleanedColumns as CQCPIRClean,
)
from utils.column_names.cleaned_data_files.ons_cleaned import (
    OnsCleanedColumns as ONSClean,
)
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys
from utils.column_values.categorical_column_values import Sector

cqc_partition_keys = [Keys.year, Keys.month, Keys.day, Keys.import_date]

cleaned_cqc_locations_columns_to_import = [
    CQCLClean.cqc_location_import_date,
    CQCLClean.location_id,
    CQCLClean.name,
    CQCLClean.postal_code,
    CQCLClean.provider_id,
    CQCLClean.cqc_sector,
    CQCLClean.imputed_registration_date,
    CQCLClean.dormancy,
    CQCLClean.care_home,
    CQCLClean.number_of_beds,
    CQCLClean.regulated_activities_offered,
    CQCLClean.services_offered,
    CQCLClean.specialisms_offered,
    CQCLClean.specialism_dementia,
    CQCLClean.specialism_learning_disabilities,
    CQCLClean.specialism_mental_health,
    CQCLClean.related_location,
    CQCLClean.primary_service_type,
    CQCLClean.primary_service_type_second_level,
    CQCLClean.registered_manager_names,
    ONSClean.contemporary_ons_import_date,
    ONSClean.contemporary_cssr,
    ONSClean.contemporary_region,
    ONSClean.contemporary_sub_icb,
    ONSClean.contemporary_icb,
    ONSClean.contemporary_icb_region,
    ONSClean.contemporary_ccg,
    ONSClean.current_ons_import_date,
    ONSClean.current_cssr,
    ONSClean.current_icb,
    ONSClean.current_region,
    ONSClean.current_lsoa21,
    ONSClean.current_msoa21,
    ONSClean.current_rural_urban_ind_11,
    Keys.year,
    Keys.month,
    Keys.day,
    Keys.import_date,
]
cleaned_ascwds_workplace_columns_to_import = [
    AWPClean.ascwds_workplace_import_date,
    AWPClean.location_id,
    AWPClean.establishment_id,
    AWPClean.organisation_id,
    AWPClean.total_staff_bounded,
    AWPClean.worker_records_bounded,
]

cleaned_cqc_pir_columns_to_import = [
    CQCPIRClean.care_home,
    CQCPIRClean.cqc_pir_import_date,
    CQCPIRClean.location_id,
    CQCPIRClean.pir_people_directly_employed_cleaned,
]

cleaned_ct_non_res_columns_to_import = [
    CTNRClean.cqc_id,
    CTNRClean.ct_non_res_import_date,
    CTNRClean.care_home,
    CTNRClean.cqc_care_workers_employed,
]

cleaned_ct_care_home_columns_to_import = [
    CTCHClean.cqc_id,
    CTCHClean.ct_care_home_import_date,
    CTCHClean.care_home,
    CTCHClean.ct_care_home_total_employed,
]


def main(
    cleaned_cqc_location_source: str,
    cleaned_cqc_pir_source: str,
    cleaned_ascwds_workplace_source: str,
    cleaned_ct_non_res_source: str,
    cleaned_ct_care_home_source: str,
    destination: str,
) -> None:
    """
    Merges ASCWDS, PIR, and capacity tracker data into the CQC locations data
    for independant sector locations only.

    Args:
        cleaned_cqc_location_source (str): s3 path to the cleaned cqc location data
        cleaned_cqc_pir_source (str): s3 path to the cleaned cqc pir data
        cleaned_ascwds_workplace_source (str): s3 path to the cleaned ascwds workplace data
        cleaned_ct_non_res_source (str): s3 path to the cleaned capacity tracker non-residential data
        cleaned_ct_care_home_source (str): s3 path to the cleaned capacity tracker care home data
        destination (str): s3 path to save the output data
    """
    cleaned_cqc_location_lf = utils.scan_parquet(
        cleaned_cqc_location_source,
        selected_columns=cleaned_cqc_locations_columns_to_import,
    )
    print("Cleaned CQC location LazyFrame read in")

    cleaned_cqc_pir_lf = utils.scan_parquet(
        cleaned_cqc_pir_source,
        cleaned_cqc_pir_source,
        selected_columns=cleaned_cqc_pir_columns_to_import,
    )
    print("Cleaned CQC PIR LazyFrame read in")

    cleaned_ascwds_workplace_lf = utils.scan_parquet(
        cleaned_ascwds_workplace_source,
        selected_columns=cleaned_ascwds_workplace_columns_to_import,
    )
    print("Cleaned ASCWDS workplace LazyFrame read in")

    cleaned_ct_non_res_lf = utils.scan_parquet(
        cleaned_ct_non_res_source, selected_columns=cleaned_ct_non_res_columns_to_import
    )
    print("Cleaned capacity tracker non-residential LazyFrame read in")

    cleaned_ct_care_home_lf = utils.scan_parquet(
        cleaned_ct_care_home_source,
        selected_columns=cleaned_ct_care_home_columns_to_import,
    )
    print("Cleaned capacity tracker care home LazyFrame read in")

    # need to test this? currently untested
    independent_cqc_lf = cleaned_cqc_location_lf.filter(
        CQCLClean.cqc_sector == Sector.independent
    )

    independent_cqc_lf = join_data_into_cqc_lf(
        independent_cqc_lf,
        cleaned_cqc_pir_lf,
        CQCPIRClean.location_id,
        CQCPIRClean.cqc_pir_import_date,
        CQCPIRClean.care_home,
    )
    print("Cleaned CQC PIR LazyFrame joined in")

    independent_cqc_lf = join_data_into_cqc_lf(
        independent_cqc_lf,
        cleaned_ascwds_workplace_lf,
        AWPClean.location_id,
        AWPClean.ascwds_workplace_import_date,
    )
    print("Cleaned ASCWDS workplace LazyFrame joined in")

    independent_cqc_lf = join_data_into_cqc_lf(
        independent_cqc_lf,
        cleaned_ct_non_res_lf,
        CTNRClean.cqc_id,
        CTNRClean.ct_non_res_import_date,
        CTNRClean.care_home,
    )
    print("Cleaned capacity tracker non-residential LazyFrame joined in")

    independent_cqc_lf = join_data_into_cqc_lf(
        independent_cqc_lf,
        cleaned_ct_care_home_lf,
        CTCHClean.cqc_id,
        CTCHClean.ct_care_home_import_date,
        CTCHClean.care_home,
    )
    print("Cleaned capacity tracker care home LazyFrame joined in")

    utils.sink_to_parquet(
        cleaned_cqc_location_lf,
        destination,
        partition_cols=cqc_partition_keys,
        append=False,
    )


if __name__ == "__main__":
    print("Running Merge Independent CQC job")

    args = utils.get_args(
        (
            "--cleaned_cqc_location_source",
            "S3 URI to read cleaned CQC location data from",
        ),
        (
            "--cleaned_cqc_pir_sourc",
            "S3 URI to read cleaned CQC PIR data from",
        ),
        (
            "--cleaned_ascwds_workplace_source",
            "S3 URI to read cleaned ASCWDS workplace data from",
        ),
        (
            "--cleaned_ct_non_res_source",
            "S3 URI to read cleaned capacity tracker non-residential data from",
        ),
        (
            "--cleaned_ct_care_home_source",
            "S3 URI to read cleaned capacity tracker care home data from",
        ),
        (
            "--destination",
            "S3 URI to save merged data to",
        ),
    )

    main(
        cleaned_cqc_location_source=args.cleaned_cqc_location_source,
        cleaned_cqc_pir_source=args.cleaned_cqc_pir_source,
        cleaned_ascwds_workplace_source=args.cleaned_ascwds_workplace_source,
        cleaned_ct_non_res_source=args.cleaned_ct_non_res_source,
        cleaned_ct_care_home_source=args.cleaned_ct_care_home_source,
        destination=args.destination,
    )

    print("Finished Merge Independent CQC job")
