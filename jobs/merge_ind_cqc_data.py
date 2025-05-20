import sys

from pyspark.sql import DataFrame
from typing import Optional

from utils import utils
import utils.cleaning_utils as cUtils
from utils.column_names.cleaned_data_files.cqc_location_cleaned import (
    CqcLocationCleanedColumns as CQCLClean,
)
from utils.column_names.cleaned_data_files.ons_cleaned import (
    OnsCleanedColumns as ONSClean,
)
from utils.column_names.cleaned_data_files.ascwds_workplace_cleaned import (
    AscwdsWorkplaceCleanedColumns as AWPClean,
)
from utils.column_names.cleaned_data_files.cqc_pir_cleaned import (
    CqcPIRCleanedColumns as CQCPIRClean,
)
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys
from utils.column_names.capacity_tracker_columns import (
    CapacityTrackerNonResCleanColumns as CTNRClean,
)
from utils.column_values.categorical_column_values import Sector


PartitionKeys = [Keys.year, Keys.month, Keys.day, Keys.import_date]

cleaned_cqc_locations_columns_to_import = [
    CQCLClean.cqc_location_import_date,
    CQCLClean.location_id,
    CQCLClean.name,
    CQCLClean.postal_code,
    CQCLClean.provider_id,
    CQCLClean.provider_name,
    CQCLClean.cqc_sector,
    CQCLClean.registration_status,
    CQCLClean.imputed_registration_date,
    CQCLClean.time_registered,
    CQCLClean.months_since_not_dormant,
    CQCLClean.dormancy,
    CQCLClean.care_home,
    CQCLClean.number_of_beds,
    CQCLClean.imputed_regulated_activities,
    CQCLClean.imputed_gac_service_types,
    CQCLClean.services_offered,
    CQCLClean.imputed_specialisms,
    CQCLClean.specialisms_offered,
    CQCLClean.related_location,
    CQCLClean.primary_service_type,
    CQCLClean.registered_manager_names,
    ONSClean.contemporary_ons_import_date,
    ONSClean.contemporary_cssr,
    ONSClean.contemporary_icb,
    ONSClean.contemporary_region,
    ONSClean.current_ons_import_date,
    ONSClean.current_cssr,
    ONSClean.current_icb,
    ONSClean.current_region,
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
    CTNRClean.capacity_tracker_import_date,
    CTNRClean.care_home,
    CTNRClean.cqc_care_workers_employed,
]


def main(
    cleaned_cqc_location_source: str,
    cleaned_cqc_pir_source: str,
    cleaned_ascwds_workplace_source: str,
    cleaned_non_res_ct_source: str,
    destination: str,
):
    spark = utils.get_spark()
    spark.sql("set spark.sql.broadcastTimeout = 2000")

    cqc_location_df = utils.read_from_parquet(
        cleaned_cqc_location_source,
        selected_columns=cleaned_cqc_locations_columns_to_import,
    )

    ascwds_workplace_df = utils.read_from_parquet(
        cleaned_ascwds_workplace_source,
        selected_columns=cleaned_ascwds_workplace_columns_to_import,
    )

    cqc_pir_df = utils.read_from_parquet(
        cleaned_cqc_pir_source, selected_columns=cleaned_cqc_pir_columns_to_import
    )

    ct_non_res_df = utils.read_from_parquet(
        cleaned_non_res_ct_source, selected_columns=cleaned_ct_non_res_columns_to_import
    )

    ind_cqc_location_df = utils.select_rows_with_value(
        cqc_location_df, CQCLClean.cqc_sector, Sector.independent
    )

    ind_cqc_location_df = join_data_into_cqc_df(
        ind_cqc_location_df,
        cqc_pir_df,
        CQCPIRClean.location_id,
        CQCPIRClean.cqc_pir_import_date,
        CQCPIRClean.care_home,
    )

    ind_cqc_location_df = join_data_into_cqc_df(
        ind_cqc_location_df,
        ascwds_workplace_df,
        AWPClean.location_id,
        AWPClean.ascwds_workplace_import_date,
    )

    ind_cqc_location_df = join_data_into_cqc_df(
        ind_cqc_location_df,
        ct_non_res_df,
        CTNRClean.cqc_id,
        CTNRClean.capacity_tracker_import_date,
        CTNRClean.care_home,
    )

    utils.write_to_parquet(
        ind_cqc_location_df,
        destination,
        mode="overwrite",
        partitionKeys=PartitionKeys,
    )


def join_data_into_cqc_df(
    cqc_df: DataFrame,
    join_df: DataFrame,
    join_location_id_col: str,
    join_import_date_col: str,
    join_care_home_col: Optional[str] = None,
) -> DataFrame:
    """
    Function to join a data file into the CQC locations data set.

    Some data needs to be matched on the care home column as well as location ID and import date, so
    there is an option to specify that. Other data doesn't require that match, so this option defaults
    to None (not required for matching).

    Args:
        cqc_df (DataFrame): The CQC location DataFrame.
        join_df (DataFrame): The DataFrame to join in.
        join_location_id_col (str): The name of the location ID column in the DataFrame to join in.
        join_import_date_col (str): The name of the import date column in the DataFrame to join in.
        join_care_home_col (Optional[str]): The name of the care home column if required for the join.

    Returns:
        DataFrame: Original CQC locations DataFrame with the second DataFrame joined in.
    """
    cqc_df_with_join_import_date = cUtils.add_aligned_date_column(
        cqc_df,
        join_df,
        CQCLClean.cqc_location_import_date,
        join_import_date_col,
    )

    join_df = join_df.withColumnRenamed(join_location_id_col, CQCLClean.location_id)

    cols_to_join_on = [join_import_date_col, CQCLClean.location_id]
    if join_care_home_col:
        cols_to_join_on = cols_to_join_on + [join_care_home_col]

    cqc_df_with_join_data = cqc_df_with_join_import_date.join(
        join_df,
        cols_to_join_on,
        "left",
    )

    return cqc_df_with_join_data


if __name__ == "__main__":
    print("Spark job 'merge_ind_cqc_data' starting...")
    print(f"Job parameters: {sys.argv}")

    (
        cleaned_cqc_location_source,
        cleaned_cqc_pir_source,
        cleaned_ascwds_workplace_source,
        cleaned_non_res_ct_source,
        destination,
    ) = utils.collect_arguments(
        (
            "--cleaned_cqc_location_source",
            "Source s3 directory for parquet CQC locations cleaned dataset",
        ),
        (
            "--cleaned_cqc_pir_source",
            "Source s3 directory for parquet CQC pir cleaned dataset",
        ),
        (
            "--cleaned_ascwds_workplace_source",
            "Source s3 directory for parquet ASCWDS workplace cleaned dataset",
        ),
        (
            "--cleaned_non_res_ct_source",
            "Source s3 directory for parquet capacity tracker cleaned dataset",
        ),
        (
            "--destination",
            "Destination s3 directory for parquet",
        ),
    )
    main(
        cleaned_cqc_location_source,
        cleaned_cqc_pir_source,
        cleaned_ascwds_workplace_source,
        cleaned_non_res_ct_source,
        destination,
    )

    print("Spark job 'merge_ind_cqc_data' complete")
