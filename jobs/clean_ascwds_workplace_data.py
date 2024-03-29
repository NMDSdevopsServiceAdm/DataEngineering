import sys

from pyspark.sql import (
    DataFrame,
    Window,
)
from pyspark.sql.types import IntegerType
import pyspark.sql.functions as F

from utils import utils
import utils.cleaning_utils as cUtils
from utils.column_names.raw_data_files.ascwds_workplace_columns import (
    PartitionKeys,
    AscwdsWorkplaceColumns as AWP,
)
from utils.column_names.cleaned_data_files.ascwds_workplace_cleaned_values import (
    AscwdsWorkplaceCleanedColumns as AWPClean,
    AscwdsWorkplaceCleanedValues as AWPValues,
)
from utils.scale_variable_limits import AscwdsScaleVariableLimits

DATE_COLUMN_IDENTIFIER = "date"
COLUMNS_TO_BOUND = [AWP.total_staff, AWP.worker_records]


def main(source: str, destination: str):
    ascwds_workplace_df = utils.read_from_parquet(source)

    ascwds_workplace_df = ascwds_workplace_df.withColumnRenamed(
        AWP.last_logged_in, AWPClean.last_logged_in_date
    )

    ascwds_workplace_df = utils.format_date_fields(
        ascwds_workplace_df,
        date_column_identifier=DATE_COLUMN_IDENTIFIER,
        raw_date_format="dd/MM/yyyy",
    )

    ascwds_workplace_df = cUtils.column_to_date(
        ascwds_workplace_df,
        PartitionKeys.import_date,
        AWPClean.ascwds_workplace_import_date,
    )

    ascwds_workplace_df = add_purge_outdated_workplaces_column(
        ascwds_workplace_df, AWPClean.ascwds_workplace_import_date
    )

    ascwds_workplace_df = purge_outdated_workplaces(ascwds_workplace_df)

    ascwds_workplace_df = remove_locations_with_duplicates(ascwds_workplace_df)

    ascwds_workplace_df = cast_to_int(ascwds_workplace_df, COLUMNS_TO_BOUND)

    ascwds_workplace_df = cUtils.set_column_bounds(
        ascwds_workplace_df,
        AWP.total_staff,
        AWPClean.total_staff_bounded,
        AscwdsScaleVariableLimits.total_staff_lower_limit,
    )

    ascwds_workplace_df = cUtils.set_column_bounds(
        ascwds_workplace_df,
        AWP.worker_records,
        AWPClean.worker_records_bounded,
        AscwdsScaleVariableLimits.worker_records_lower_limit,
    )

    print(f"Exporting as parquet to {destination}")
    utils.write_to_parquet(
        ascwds_workplace_df,
        destination,
        mode="overwrite",
        partitionKeys=[
            PartitionKeys.year,
            PartitionKeys.month,
            PartitionKeys.day,
            PartitionKeys.import_date,
        ],
    )


def cast_to_int(df: DataFrame, column_names: list) -> DataFrame:
    for column in column_names:
        df = df.withColumn(column, df[column].cast(IntegerType()))
    return df


def remove_locations_with_duplicates(df: DataFrame):
    loc_id_import_date_window = Window.partitionBy(
        AWP.location_id, PartitionKeys.import_date
    )

    df_with_count = df.withColumn(
        "location_id_count", F.count(AWP.location_id).over(loc_id_import_date_window)
    )

    df_without_duplicates = df_with_count.filter(F.col("location_id_count") == 1)

    return df_without_duplicates.drop("location_id_count")


def add_purge_outdated_workplaces_column(
    df: DataFrame, comparison_date_col: str
) -> DataFrame:
    """
    For a given ascwds_workplace_df, based on the comparison_date_col, returns a dataframe where each row is marked to keep or purge in a new column

    The rough steps are outlined below:
    - Adds a column of purge dates which is a number of months before the comparison_date_col
    - Calculates the latest update using an external function
    - Compares this latest update date to the purge date, and marks the row accordingly, and clears the date information

    Args:
        df (DataFrame): An ascwds_workplace_df that must contain at least the comparison_date_col
        comparison_date_col (str): The data column name to make comparisons on

    Returns:
        final_df (DataFrame): a dataframe where each row is marked for keeping or purging

    """
    MONTHS_BEFORE_COMPARISON_DATE_TO_PURGE = 24

    df_with_purge_date = df.withColumn(
        "purge_date",
        F.add_months(
            F.col(comparison_date_col), -MONTHS_BEFORE_COMPARISON_DATE_TO_PURGE
        ),
    )

    df_with_latest_update = calculate_latest_update_to_workplace_location(
        df_with_purge_date, comparison_date_col
    )

    df_with_purge_data = df_with_latest_update.withColumn(
        AWPClean.purge_data,
        F.when(
            (F.col("latest_update") < F.col("purge_date")), AWPValues.purge_delete
        ).otherwise(AWPValues.purge_keep),
    )

    final_df = df_with_purge_data.drop("purge_date", "latest_update")

    return final_df


def calculate_latest_update_to_workplace_location(df: DataFrame, comparison_date_col):
    org_df_with_latest_updates = df.groupBy(
        AWP.organisation_id, comparison_date_col
    ).agg(F.max(AWP.master_update_date).alias("latest_org_mupddate"))

    df_with_org_updates = df.join(
        org_df_with_latest_updates, [AWP.organisation_id, comparison_date_col], "left"
    )

    df_with_latest_update = df_with_org_updates.withColumn(
        "latest_update",
        F.when((F.col(AWP.is_parent) == "1"), F.col("latest_org_mupddate")).otherwise(
            F.col(AWP.master_update_date)
        ),
    )

    df_with_latest_update = df_with_latest_update.drop("latest_org_mupddate")

    return df_with_latest_update


def purge_outdated_workplaces(
    ascwds_workplace_df_with_purge_marker: DataFrame,
    purge_marker_col: str = AWPClean.purge_data,
) -> DataFrame:
    """
    Takes an ascwds_workplace_df extended with a column that marks data for purging or keeping
    (such as the output from add_purge_outdated_workplaces_column),
    and filters the dataframe based on rows marked to keep

    Args:
        ascwds_workplace_df_with_purge_marker (DataFrame): As the name suggests, a DataFrame of workplace data with the purge marker present
        purge_marker_col (str): (Default = AWPClean.purge_data) This is the name of the column where the purge marker is located

    Returns:
        The dataframe but now filtered to only rows where the marker indicated the row be kept.
    """
    return ascwds_workplace_df_with_purge_marker.filter(
        F.col(purge_marker_col) == AWPValues.purge_keep
    )


if __name__ == "__main__":
    print("Spark job 'ingest_ascwds_workplace_dataset' starting...")
    print(f"Job parameters: {sys.argv}")

    source, destination = utils.collect_arguments(
        (
            "--ascwds_workplace_source",
            "Source s3 directory for parquet ascwds workplace dataset",
        ),
        (
            "--ascwds_workplace_destination",
            "Destination s3 directory for cleaned parquet ascwds workplace dataset",
        ),
    )
    main(source, destination)

    print("Spark job 'ingest_ascwds_dataset' complete")
