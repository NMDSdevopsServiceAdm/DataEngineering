import os
import sys

os.environ["SPARK_VERSION"] = "3.5"

from typing import Optional

from pyspark.sql import DataFrame

import utils.cleaning_utils as cUtils
from utils import utils
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
from utils.column_names.ind_cqc_pipeline_columns import (
    DimensionPartitionKeys as DimensionKeys,
)
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys
from utils.column_values.categorical_column_values import RegistrationStatus, Sector

PartitionKeys = [Keys.year, Keys.month, Keys.day, Keys.import_date]

cleaned_cqc_locations_columns_to_import = [
    CQCLClean.cqc_location_import_date,
    CQCLClean.location_id,
    CQCLClean.name,
    CQCLClean.provider_id,
    CQCLClean.cqc_sector,
    CQCLClean.registration_status,
    CQCLClean.imputed_registration_date,
    CQCLClean.dormancy,
    CQCLClean.number_of_beds,
    CQCLClean.related_location,
    Keys.year,
    Keys.month,
    Keys.day,
    Keys.import_date,
]
gac_dim_columns_to_import = [
    CQCLClean.location_id,
    CQCLClean.imputed_gac_service_types,
    CQCLClean.services_offered,
    CQCLClean.primary_service_type,
    CQCLClean.care_home,
    Keys.import_date,
    DimensionKeys.last_updated,
]
ra_dim_columns_to_import = [
    CQCLClean.location_id,
    CQCLClean.imputed_regulated_activities,
    CQCLClean.registered_manager_names,
    Keys.import_date,
    DimensionKeys.last_updated,
]
specialism_dim_columns_to_import = [
    CQCLClean.location_id,
    CQCLClean.imputed_specialisms,
    CQCLClean.specialisms_offered,
    CQCLClean.specialist_generalist_other_dementia,
    CQCLClean.specialist_generalist_other_lda,
    CQCLClean.specialist_generalist_other_mh,
    Keys.import_date,
    DimensionKeys.last_updated,
]
pcm_dim_columns_to_import = [
    CQCLClean.location_id,
    CQCLClean.postal_code,
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
    ONSClean.current_rural_urban_ind_11,
    Keys.import_date,
    DimensionKeys.last_updated,
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
    gac_dim_source: str,
    reg_act_dim_source: str,
    spec_dim_source: str,
    pcm_dim_source: str,
    cleaned_cqc_pir_source: str,
    cleaned_ascwds_workplace_source: str,
    cleaned_ct_non_res_source: str,
    cleaned_ct_care_home_source: str,
    destination: str,
):
    spark = utils.get_spark()
    spark.sql("set spark.sql.broadcastTimeout = 2000")

    # TODO - Polars conversion - apply .fliter on scan_parquet instead of reading full dataframe and then filtering
    cqc_location_df = utils.read_from_parquet(
        cleaned_cqc_location_source,
        selected_columns=cleaned_cqc_locations_columns_to_import,
    )
    cqc_registered_df = utils.select_rows_with_value(
        cqc_location_df, CQCLClean.registration_status, RegistrationStatus.registered
    )
    ind_cqc_registered_df = utils.select_rows_with_value(
        cqc_registered_df, CQCLClean.cqc_sector, Sector.independent
    )

    gac_dim_df = utils.read_from_parquet(
        gac_dim_source,
        selected_columns=gac_dim_columns_to_import,
    )
    ind_cqc_registered_df = utils.join_dimension(
        ind_cqc_registered_df, gac_dim_df, CQCLClean.location_id
    )

    reg_act_dim_df = utils.read_from_parquet(
        reg_act_dim_source, selected_columns=ra_dim_columns_to_import
    )
    ind_cqc_registered_df = utils.join_dimension(
        ind_cqc_registered_df, reg_act_dim_df, CQCLClean.location_id
    )

    spec_dim_df = utils.read_from_parquet(
        spec_dim_source, selected_columns=specialism_dim_columns_to_import
    )
    ind_cqc_registered_df = utils.join_dimension(
        ind_cqc_registered_df, spec_dim_df, CQCLClean.location_id
    )

    pcm_dim_df = utils.read_from_parquet(
        pcm_dim_source, selected_columns=pcm_dim_columns_to_import
    )
    ind_cqc_registered_df = utils.join_dimension(
        ind_cqc_registered_df, pcm_dim_df, CQCLClean.location_id
    )

    ascwds_workplace_df = utils.read_from_parquet(
        cleaned_ascwds_workplace_source,
        selected_columns=cleaned_ascwds_workplace_columns_to_import,
    )

    cqc_pir_df = utils.read_from_parquet(
        cleaned_cqc_pir_source, selected_columns=cleaned_cqc_pir_columns_to_import
    )

    ct_non_res_df = utils.read_from_parquet(
        cleaned_ct_non_res_source, selected_columns=cleaned_ct_non_res_columns_to_import
    )

    ct_care_home_df = utils.read_from_parquet(
        cleaned_ct_care_home_source,
        selected_columns=cleaned_ct_care_home_columns_to_import,
    )

    ind_cqc_registered_df = join_data_into_cqc_df(
        ind_cqc_registered_df,
        cqc_pir_df,
        CQCPIRClean.location_id,
        CQCPIRClean.cqc_pir_import_date,
        CQCPIRClean.care_home,
    )

    ind_cqc_registered_df = join_data_into_cqc_df(
        ind_cqc_registered_df,
        ascwds_workplace_df,
        AWPClean.location_id,
        AWPClean.ascwds_workplace_import_date,
    )

    ind_cqc_registered_df = join_data_into_cqc_df(
        ind_cqc_registered_df,
        ct_non_res_df,
        CTNRClean.cqc_id,
        CTNRClean.ct_non_res_import_date,
        CTNRClean.care_home,
    )

    ind_cqc_registered_df = join_data_into_cqc_df(
        ind_cqc_registered_df,
        ct_care_home_df,
        CTCHClean.cqc_id,
        CTCHClean.ct_care_home_import_date,
        CTCHClean.care_home,
    )

    utils.write_to_parquet(
        ind_cqc_registered_df,
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
        gac_services_source,
        regulated_activities_source,
        specialisms_source,
        postcode_matching_source,
        cleaned_cqc_pir_source,
        cleaned_ascwds_workplace_source,
        cleaned_ct_non_res_source,
        cleaned_ct_care_home_source,
        destination,
    ) = utils.collect_arguments(
        (
            "--cleaned_cqc_location_source",
            "Source s3 directory for parquet CQC locations cleaned dataset",
        ),
        (
            "--gac_services_dimension_source",
            "Source s3 directory for parquet GAC services dimension",
        ),
        (
            "--regulated_activities_dimension_source",
            "Source s3 directory for parquet Regulated Activities dimension",
        ),
        (
            "--specialisms_dimension_source",
            "Source s3 directory for parquet Services dimension",
        ),
        (
            "--postcode_matching_dimension_source",
            "Source s3 directory for parquet Postcode Matching dimension",
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
            "--cleaned_ct_non_res_source",
            "Source s3 directory for parquet capacity tracker non residential cleaned dataset",
        ),
        (
            "--cleaned_ct_care_home_source",
            "Source s3 directory for parquet capacity tracker care home cleaned dataset",
        ),
        (
            "--destination",
            "Destination s3 directory for parquet",
        ),
    )
    main(
        cleaned_cqc_location_source,
        gac_services_source,
        regulated_activities_source,
        specialisms_source,
        postcode_matching_source,
        cleaned_cqc_pir_source,
        cleaned_ascwds_workplace_source,
        cleaned_ct_non_res_source,
        cleaned_ct_care_home_source,
        destination,
    )

    print("Spark job 'merge_ind_cqc_data' complete")
