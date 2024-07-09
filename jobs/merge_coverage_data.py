import sys

from pyspark.sql import DataFrame, functions as F

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
from utils.column_names.ind_cqc_pipeline_columns import (
    PartitionKeys as Keys,
)
from utils.column_names.coverage_columns import CoverageColumns

from utils.column_values.categorical_column_values import InAscwds

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
    CQCLClean.gac_service_types,
    CQCLClean.services_offered,
    CQCLClean.specialisms,
    CQCLClean.primary_service_type,
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
    AWPClean.total_staff,
    AWPClean.worker_records,
]


def main(
    cleaned_cqc_location_source: str,
    workplace_for_reconciliation_source: str,
    merged_coverage_destination: str,
):
    spark = utils.get_spark()
    spark.sql(
        "set spark.sql.broadcastTimeout = 2000"
    )  # TODO: Test if this is needed still

    cqc_location_df = utils.read_from_parquet(
        cleaned_cqc_location_source,
        selected_columns=cleaned_cqc_locations_columns_to_import,
    )

    ascwds_workplace_df = utils.read_from_parquet(
        workplace_for_reconciliation_source,
        selected_columns=cleaned_ascwds_workplace_columns_to_import,
    )

    merged_coverage_df = join_ascwds_data_into_cqc_location_df(
        cqc_location_df,
        ascwds_workplace_df,
        CQCLClean.cqc_location_import_date,
        AWPClean.ascwds_workplace_import_date,
    )

    merged_coverage_df = add_flag_for_in_ascwds(merged_coverage_df)

    utils.write_to_parquet(
        merged_coverage_df,
        merged_coverage_destination,
        mode="overwrite",
        partitionKeys=PartitionKeys,
    )


def join_ascwds_data_into_cqc_location_df(
    cqc_location_df: DataFrame,
    ascwds_workplace_df: DataFrame,
    cqc_location_import_date_column: str,
    ascwds_workplace_import_date_column: str,
) -> DataFrame:
    """
    Joins ASC-WDS reconciliation data to CQC locations.

    Takes specific columns from the cleaned CQC locations dataframe and joins specific columns from the ASC-WDS reconciliation dataframe.

    Args:
        cqc_location_df (DataFrame): A dataframe of cleaned CQC locations.
        ascwds_workplace_df (DataFrame): A dataframe of ASC-WDS workplaces which includes workplaces last updated or logged into within 2 years of snapshot.
        cqc_location_import_date_column (String): A string refering to the import date column in the clean CQC locations dataframe.
        ascwds_workplace_import_date_column (String): A string refering to the import date column in the ASC-WDS reconciliation dataframe.

    Returns:
        DataFrame: The clean CQC locations dataframe with all columns from the ASC-WDS reconciliation dataframe added to it.
    """
    merged_coverage_ascwds_df_with_ascwds_workplace_import_date = (
        cUtils.add_aligned_date_column(
            cqc_location_df,
            ascwds_workplace_df,
            cqc_location_import_date_column,
            ascwds_workplace_import_date_column,
        )
    )

    formatted_ascwds_workplace_df = ascwds_workplace_df.withColumnRenamed(
        AWPClean.location_id, CQCLClean.location_id
    )

    merged_coverage_df = (
        merged_coverage_ascwds_df_with_ascwds_workplace_import_date.join(
            formatted_ascwds_workplace_df,
            [CQCLClean.location_id, AWPClean.ascwds_workplace_import_date],
            how="left",
        )
    )

    return merged_coverage_df


def add_flag_for_in_ascwds(
    merged_coverage_df: DataFrame,
) -> DataFrame:
    """
    Add a column to the merged coverage dataframe which flags if CQC location is in ASC-WDS.

    When row has an ASC-WDS establishmentid then value is 1, otherwise value is 0.

    Args:
        merged_coverage_df (DataFrame): A dataframe of CQC locations with ASC-WDS columns joined via locationid.

    Returns:
        DataFrame: A dataframe with an additional column that flags if CQC location is in ASC-WDS.
    """
    merged_coverage_df = merged_coverage_df.withColumn(
        CoverageColumns.in_ascwds,
        F.when(
            F.isnull(AWPClean.establishment_id),
            InAscwds.not_in_ascwds,
        ).otherwise(InAscwds.is_in_ascwds),
    )

    return merged_coverage_df


if __name__ == "__main__":
    print("Spark job 'merge_coverage_data' starting...")
    print(f"Job parameters: {sys.argv}")

    (
        cleaned_cqc_location_source,
        workplace_for_reconciliation_source,
        merged_coverage_destination,
    ) = utils.collect_arguments(
        (
            "--cleaned_cqc_location_source",
            "Source s3 directory for parquet CQC locations cleaned dataset",
        ),
        (
            "--workplace_for_reconciliation_source",
            "Source s3 directory for parquet ASCWDS workplace for reconciliation dataset",
        ),
        (
            "--merged_coverage_destination",
            "Destination s3 directory for parquet",
        ),
    )
    main(
        cleaned_cqc_location_source,
        workplace_for_reconciliation_source,
        merged_coverage_destination,
    )

    print("Spark job 'merge_coverage_data' complete")
