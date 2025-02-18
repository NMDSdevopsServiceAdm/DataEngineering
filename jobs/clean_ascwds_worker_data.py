import sys

from pyspark.sql import DataFrame, functions as F, Window

from utils import utils
import utils.cleaning_utils as cUtils
from utils.raw_data_adjustments import remove_duplicate_worker_in_raw_worker_data
from utils.column_names.raw_data_files.ascwds_worker_columns import PartitionKeys
from utils.column_names.cleaned_data_files.ascwds_worker_cleaned import (
    AscwdsWorkerCleanedColumns as AWKClean,
)
from utils.column_names.cleaned_data_files.ascwds_workplace_cleaned import (
    AscwdsWorkplaceCleanedColumns as AWPClean,
)
from utils.value_labels.ascwds_worker.worker_label_dictionary import (
    ascwds_worker_labels_dict,
)

WORKER_COLUMNS = [
    AWKClean.location_id,
    AWKClean.worker_id,
    AWKClean.import_date,
    AWKClean.establishment_id,
    AWKClean.main_job_role_id,
    AWKClean.year,
    AWKClean.month,
    AWKClean.day,
]

WORKPLACE_COLUMNS = [
    AWPClean.import_date,
    AWPClean.establishment_id,
]


def main(
    worker_source: str, cleaned_workplace_source: str, cleaned_worker_destination: str
):
    ascwds_worker_df = utils.read_from_parquet(
        worker_source,
        WORKER_COLUMNS,
    )
    ascwds_workplace_cleaned_df = utils.read_from_parquet(
        cleaned_workplace_source,
        WORKPLACE_COLUMNS,
    )

    ascwds_worker_df = cUtils.column_to_date(
        ascwds_worker_df, PartitionKeys.import_date, AWKClean.ascwds_worker_import_date
    )

    ascwds_worker_df = remove_duplicate_worker_in_raw_worker_data(ascwds_worker_df)

    ascwds_worker_df = remove_workers_without_workplaces(
        ascwds_worker_df, ascwds_workplace_cleaned_df
    )

    ascwds_worker_df = create_clean_main_job_role_column(ascwds_worker_df)

    print(f"Exporting as parquet to {cleaned_worker_destination}")
    utils.write_to_parquet(
        ascwds_worker_df,
        cleaned_worker_destination,
        mode="overwrite",
        partitionKeys=[
            PartitionKeys.year,
            PartitionKeys.month,
            PartitionKeys.day,
            PartitionKeys.import_date,
        ],
    )


def remove_workers_without_workplaces(
    worker_df: DataFrame, workplace_df: DataFrame
) -> DataFrame:
    """
    Removes worker records that do not have a corresponding workplace record.

    Workplaces are cleaned during the workplace cleaning process, so if a workplace has been removed then the worker records for that workplace should also be removed.

    Args:
        worker_df (DataFrame): The DataFrame containing the worker records.
        workplace_df (DataFrame): The DataFrame containing the workplace records.

    Returns:
        DataFrame: The DataFrame with only the worker records that have a corresponding workplace record.
    """
    workplace_df = workplace_df.select(
        [AWPClean.import_date, AWPClean.establishment_id]
    )

    return worker_df.join(
        workplace_df, [AWKClean.import_date, AWKClean.establishment_id], "inner"
    )


def create_clean_main_job_role_column(df: DataFrame) -> DataFrame:
    """
    Contains the steps to create the clean the main job role column and and the categorical labels as a new column.

    Args:
        df (DataFrame): The DataFrame containing the original main job role column.

    Returns:
        DataFrame: The DataFrame with the cleaned main job role column .
    """
    df = df.withColumn(AWKClean.main_job_role_clean, F.col(AWKClean.main_job_role_id))

    df = replace_care_navigator_with_care_coordinator(df)
    df = impute_not_known_job_roles(df)
    df = remove_workers_with_not_known_job_role(df)
    df = cUtils.apply_categorical_labels(
        df,
        ascwds_worker_labels_dict,
        ascwds_worker_labels_dict.keys(),
        add_as_new_column=True,
    )
    return df


def replace_care_navigator_with_care_coordinator(df: DataFrame) -> DataFrame:
    """
    Replaces 'Care Navigator' ("41") with 'Care Co-ordinator' ("40") in the main job role column.

    In May 2024, the job role 'Care Navigator' was removed from ASC-WDS and all workers in ASC-WDS in that role at the time were moved to the 'Care Co-ordinator' role.
    This function backdates this change to the start of the dataset for consistency.

    Args:
        df (DataFrame): The DataFrame containing the main job role column.

    Returns:
        DataFrame: The DataFrame with the replaced value.
    """
    return df.replace("41", "40", AWKClean.main_job_role_clean)


def impute_not_known_job_roles(df: DataFrame) -> DataFrame:
    """
    Imputes not known job roles in the DataFrame by filling with known values from other import_dates.

    The function performs the following steps:
    1. Replaces 'not known' (labelled as '-1') job roles with None.
    2. Fills the None values in with the previous known value within the partition.
    3. Fills any remaining None values with the future known value within the partition.
    4. Replaces missing job role rows with the original 'not known' ('-1') value.

    Args:
        df (DataFrame): Input DataFrame containing 'worker_id', 'ascwds_worker_import_date' and 'main_job_role_clean'.

    Returns:
        DataFrame: DataFrame with the 'main_job_role_clean' column with imputed values.
    """
    w_future = Window.partitionBy(AWKClean.worker_id).orderBy(
        AWKClean.ascwds_worker_import_date
    )
    w_historic = w_future.rowsBetween(
        Window.unboundedPreceding, Window.unboundedFollowing
    )
    not_known_identifier: str = "-1"

    df = df.replace(not_known_identifier, None, AWKClean.main_job_role_clean)
    df = df.withColumn(
        AWKClean.main_job_role_clean,
        F.coalesce(
            F.col(AWKClean.main_job_role_clean),
            F.last(AWKClean.main_job_role_clean, ignorenulls=True).over(w_future),
        ),
    )
    df = df.withColumn(
        AWKClean.main_job_role_clean,
        F.coalesce(
            F.col(AWKClean.main_job_role_clean),
            F.first(AWKClean.main_job_role_clean, ignorenulls=True).over(w_historic),
        ),
    )
    df = df.na.fill(not_known_identifier, subset=AWKClean.main_job_role_clean)
    return df


def remove_workers_with_not_known_job_role(df: DataFrame) -> DataFrame:
    """
    Removes worker records with a main job role of 'not known' ('-1').

    Args:
        df (DataFrame): The DataFrame containing the worker records.

    Returns:
        DataFrame: The DataFrame with the worker records that have a main job role other than 'not known' ('-1').
    """
    return df.filter(F.col(AWKClean.main_job_role_clean) != "-1")


if __name__ == "__main__":
    print("Spark job 'ingest_ascwds_worker_dataset' starting...")
    print(f"Job parameters: {sys.argv}")

    (
        worker_source,
        cleaned_workplace_source,
        cleaned_worker_destination,
    ) = utils.collect_arguments(
        (
            "--ascwds_worker_source",
            "Source s3 directory for parquet ascwds worker dataset",
        ),
        (
            "--ascwds_workplace_cleaned_source",
            "Source s3 directory for parquet ascwds workplace cleaned dataset",
        ),
        (
            "--ascwds_worker_destination",
            "Destination s3 directory for cleaned parquet ascwds worker dataset",
        ),
    )
    main(worker_source, cleaned_workplace_source, cleaned_worker_destination)

    print("Spark job 'ingest_ascwds_dataset' complete")
