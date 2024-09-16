import sys

from pyspark.sql import DataFrame, functions as F

from utils import utils
import utils.cleaning_utils as cUtils
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
    PartitionKeys as Keys,
)
from utils.column_values.categorical_column_values import Sector


PartitionKeys = [Keys.year, Keys.month, Keys.day, Keys.import_date]

cleaned_cqc_locations_columns_to_import = [
    CQCLClean.cqc_location_import_date,
    CQCLClean.location_id,
    CQCLClean.name,
    CQCLClean.provider_id,
    CQCLClean.provider_name,
    CQCLClean.cqc_sector,
    CQCLClean.registration_status,
    CQCLClean.imputed_registration_date,
    CQCLClean.dormancy,
    CQCLClean.care_home,
    CQCLClean.number_of_beds,
    CQCLClean.regulated_activities,
    CQCLClean.imputed_gac_service_types,
    CQCLClean.services_offered,
    CQCLClean.specialisms,
    CQCLClean.related_location,
    CQCLClean.primary_service_type,
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
    CQCPIRClean.people_directly_employed,
]


def main(
    cleaned_cqc_location_source: str,
    cleaned_cqc_pir_source: str,
    cleaned_ascwds_workplace_source: str,
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

    ind_cqc_location_df = filter_df_to_independent_sector_only(cqc_location_df)

    ind_cqc_location_df = join_pir_data_into_merged_df(ind_cqc_location_df, cqc_pir_df)

    ind_cqc_location_df = join_ascwds_data_into_merged_df(
        ind_cqc_location_df,
        ascwds_workplace_df,
        CQCLClean.cqc_location_import_date,
        AWPClean.ascwds_workplace_import_date,
    )

    utils.write_to_parquet(
        ind_cqc_location_df,
        destination,
        mode="overwrite",
        partitionKeys=PartitionKeys,
    )


def filter_df_to_independent_sector_only(df: DataFrame) -> DataFrame:
    return df.where(F.col(CQCLClean.cqc_sector) == Sector.independent)


def join_pir_data_into_merged_df(ind_df: DataFrame, pir_df: DataFrame):
    ind_df_with_pir_import_date = cUtils.add_aligned_date_column(
        ind_df,
        pir_df,
        CQCLClean.cqc_location_import_date,
        CQCPIRClean.cqc_pir_import_date,
    )

    formatted_pir_df = pir_df.withColumnRenamed(
        CQCPIRClean.location_id, CQCLClean.location_id
    ).withColumnRenamed(CQCPIRClean.care_home, CQCLClean.care_home)

    ind_df_with_pir_data = ind_df_with_pir_import_date.join(
        formatted_pir_df,
        [CQCPIRClean.cqc_pir_import_date, CQCLClean.location_id, CQCLClean.care_home],
        "left",
    )

    return ind_df_with_pir_data


def join_ascwds_data_into_merged_df(
    ind_cqc_df: DataFrame,
    ascwds_workplace_df: DataFrame,
    ind_cqc_import_date_column: str,
    ascwds_workplace_import_date_column: str,
) -> DataFrame:
    ind_cqc_df_with_ascwds_workplace_import_date = cUtils.add_aligned_date_column(
        ind_cqc_df,
        ascwds_workplace_df,
        ind_cqc_import_date_column,
        ascwds_workplace_import_date_column,
    )

    formatted_ascwds_workplace_df = ascwds_workplace_df.withColumnRenamed(
        AWPClean.location_id, CQCLClean.location_id
    )

    ind_cqc_with_ascwds_df = ind_cqc_df_with_ascwds_workplace_import_date.join(
        formatted_ascwds_workplace_df,
        [CQCLClean.location_id, AWPClean.ascwds_workplace_import_date],
        how="left",
    )

    return ind_cqc_with_ascwds_df


if __name__ == "__main__":
    print("Spark job 'merge_ind_cqc_data' starting...")
    print(f"Job parameters: {sys.argv}")

    (
        cleaned_cqc_location_source,
        cleaned_cqc_pir_source,
        cleaned_ascwds_workplace_source,
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
            "--destination",
            "Destination s3 directory for parquet",
        ),
    )
    main(
        cleaned_cqc_location_source,
        cleaned_cqc_pir_source,
        cleaned_ascwds_workplace_source,
        destination,
    )

    print("Spark job 'merge_ind_cqc_data' complete")
