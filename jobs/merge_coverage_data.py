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
from utils.column_names.cqc_ratings_columns import CQCRatingsColumns
from utils.column_values.categorical_column_values import (
    CQCLatestRating,
    InAscwds,
    CQCCurrentOrHistoricValues,
)

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
    CQCLClean.dormancy,
    CQCLClean.care_home,
    CQCLClean.number_of_beds,
    CQCLClean.regulated_activities,
    CQCLClean.imputed_gac_service_types,
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
    AWPClean.master_update_date,
    AWPClean.master_update_date_org,
    AWPClean.establishment_created_date,
    AWPClean.nmds_id,
    AWPClean.last_logged_in,
    AWPClean.is_parent,
    AWPClean.parent_permission,
]
cqc_ratings_columns_to_import = [
    AWPClean.location_id,
    CQCRatingsColumns.date,
    CQCRatingsColumns.overall_rating,
    CQCRatingsColumns.latest_rating_flag,
    CQCRatingsColumns.current_or_historic,
]


def main(
    cleaned_cqc_location_source: str,
    workplace_for_reconciliation_source: str,
    cqc_ratings_source: str,
    merged_coverage_destination: str,
    reduced_coverage_destination: str,
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

    cqc_ratings_df = utils.read_from_parquet(
        cqc_ratings_source,
        selected_columns=cqc_ratings_columns_to_import,
    )

    ascwds_workplace_df = cUtils.remove_duplicates_based_on_column_order(
        ascwds_workplace_df,
        [AWPClean.ascwds_workplace_import_date, AWPClean.location_id],
        AWPClean.master_update_date,
        sort_ascending=False,
    )

    merged_coverage_df = join_ascwds_data_into_cqc_location_df(
        cqc_location_df,
        ascwds_workplace_df,
        CQCLClean.cqc_location_import_date,
        AWPClean.ascwds_workplace_import_date,
    )

    merged_coverage_df = add_flag_for_in_ascwds(merged_coverage_df)

    merged_coverage_df = cUtils.remove_duplicates_based_on_column_order(
        merged_coverage_df,
        [
            CQCLClean.cqc_location_import_date,
            CQCLClean.name,
            CQCLClean.postal_code,
            CQCLClean.care_home,
        ],
        CoverageColumns.in_ascwds,
        sort_ascending=False,
    )

    merged_coverage_df = join_latest_cqc_rating_into_coverage_df(
        merged_coverage_df, cqc_ratings_df
    )

    utils.write_to_parquet(
        merged_coverage_df,
        merged_coverage_destination,
        mode="overwrite",
        partitionKeys=PartitionKeys,
    )

    reduced_coverage_df = cUtils.reduce_dataset_to_earliest_file_per_month(
        merged_coverage_df
    )
    reduced_coverage_df = utils.filter_df_to_maximum_value_in_column(
        reduced_coverage_df, CQCLClean.cqc_location_import_date
    )

    utils.write_to_parquet(
        reduced_coverage_df,
        reduced_coverage_destination,
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

    Requirements that are not arguments: CQC locationid.
    Takes the cleaned CQC locations dataframe, looks at it's import date column, and adds a new column which is the aligned import date from ASC-WDS reconciliation dataframe.
    The ASC-WDS reconciliation import date added will be equal to or before the cleaned CQC locations import date.
    Takes all columns from the cleaned CQC locations dataframe and joins all columns from the ASC-WDS reconciliation dataframe.
    Dataframe's are joined using locatoinid and aligned import date.

    Args:
        cqc_location_df (DataFrame): A dataframe of cleaned CQC locations.
        ascwds_workplace_df (DataFrame): A dataframe of ASC-WDS workplaces which includes workplaces last updated or logged into within 2 years of snapshot.
        cqc_location_import_date_column (str): The name of the import date column in the clean CQC locations dataframe.
        ascwds_workplace_import_date_column (str): The name of the import date column in the ASC-WDS reconciliation dataframe.

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

    Requirements which are not arguments: ASC-WDS establishmentid.
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


def filter_for_latest_cqc_ratings(
    cqc_ratings_df: DataFrame,
) -> DataFrame:
    """
    Filter the CQC ratings dataframe to latest rating per location only.

    Requirements that are not arguments: latest_rating_flag, current_or_historic.
    The CQC ratings dataset shows 1 for the latest rating in the latest_rating_flag column.
    This function removes rows from the cqc ratings dataframe when latest_rating_flag = 0 and current_or_historic = 'current'.

    Args:
        cqc_ratings_df (DataFrame): A dataframe of cqc ratings.

    Returns:
        DataFrame: The cqc ratings dataframe with only the latest current rating per location.
    """
    cqc_ratings_df = cqc_ratings_df.dropDuplicates()
    cqc_ratings_df = cqc_ratings_df.where(
        (
            cqc_ratings_df[CQCRatingsColumns.latest_rating_flag]
            == CQCLatestRating.is_latest_rating
        )
        & (
            cqc_ratings_df[CQCRatingsColumns.current_or_historic]
            == CQCCurrentOrHistoricValues.current
        )
    )
    return cqc_ratings_df


def join_latest_cqc_rating_into_coverage_df(
    merged_coverage_df: DataFrame,
    cqc_ratings_df: DataFrame,
) -> DataFrame:
    """
    Join latest rating to coverage dataframe using locationid as key.

    Requirements that are not arguments: CQC locationid.
    Columns from the CQC ratings dataframe are joined to the coverage dataframe using locationid. The cqc ratings
    data pulled through will contain duplicates, because it is a subset of the columns. This has been addressed in
    the filtering function.

    Args:
        merged_coverage_df (DataFrame): A dataframe of CQC locations with ASC-WDS columns joined via locationid.
        cqc_ratings_df (DataFrame): A dataframe of cqc ratings.

    Returns:
        DataFrame: The coverage dataframe with the latest overall CQC rating and the rating date added to it.
    """

    latest_cqc_ratings_df = filter_for_latest_cqc_ratings(cqc_ratings_df)

    merged_coverage_with_latest_rating_df = merged_coverage_df.join(
        latest_cqc_ratings_df,
        CQCLClean.location_id,
        how="left",
    )

    merged_coverage_with_latest_rating_df = merged_coverage_with_latest_rating_df.drop(
        CQCRatingsColumns.latest_rating_flag
    )
    merged_coverage_with_latest_rating_df = merged_coverage_with_latest_rating_df.drop(
        CQCRatingsColumns.current_or_historic
    )
    return merged_coverage_with_latest_rating_df


if __name__ == "__main__":
    print("Spark job 'merge_coverage_data' starting...")
    print(f"Job parameters: {sys.argv}")

    (
        cleaned_cqc_location_source,
        workplace_for_reconciliation_source,
        cqc_ratings_source,
        merged_coverage_destination,
        reduced_coverage_destination,
    ) = utils.collect_arguments(
        (
            "--cleaned_cqc_location_source",
            "Source s3 directory for parquet CQC locations cleaned dataset",
        ),
        (
            "--workplace_for_reconciliation_source",
            "Source s3 directory for parquet ASCWDS workplace for reconciliation dataset",
        ),
        ("--cqc_ratings_source", "Source s3 directory for parquet CQC ratings dataset"),
        (
            "--merged_coverage_destination",
            "Destination s3 directory for full parquet",
        ),
        (
            "--reduced_coverage_destination",
            "Destination s3 directory for single month parquet",
        ),
    )
    main(
        cleaned_cqc_location_source,
        workplace_for_reconciliation_source,
        cqc_ratings_source,
        merged_coverage_destination,
        reduced_coverage_destination,
    )

    print("Spark job 'merge_coverage_data' complete")
