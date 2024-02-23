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

DATE_COLUMN_IDENTIFIER = "date"


def main(source: str, destination: str):
    ascwds_workplace_df = utils.read_from_parquet(source)

    ascwds_workplace_df = cUtils.column_to_date(
        ascwds_workplace_df,
        PartitionKeys.import_date,
        AWPClean.ascwds_workplace_import_date,
    )

    ascwds_workplace_df = ascwds_workplace_df.withColumnRenamed(
        AWP.last_logged_in, AWPClean.last_logged_in_date
    )

    ascwds_workplace_df = utils.format_date_fields(
        ascwds_workplace_df,
        date_column_identifier=DATE_COLUMN_IDENTIFIER,
        raw_date_format="dd/MM/yyyy",
    )

    ascwds_workplace_df = add_purge_outdated_workplaces_column(
        ascwds_workplace_df, AWPClean.ascwds_workplace_import_date
    )

    ascwds_workplace_df = remove_locations_with_duplicates(ascwds_workplace_df)

    ascwds_workplace_df = cast_to_int(
        ascwds_workplace_df, [AWP.total_staff, AWP.worker_records]
    )

    ascwds_workplace_df = create_column_with_repeated_values_removed(
        ascwds_workplace_df,
        AWP.total_staff,
    )
    ascwds_workplace_df = create_column_with_repeated_values_removed(
        ascwds_workplace_df,
        AWP.worker_records,
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


def create_column_with_repeated_values_removed(
    df: DataFrame,
    column_to_clean: str,
    new_column_name: str = None,
) -> DataFrame:
    """
    ASCWDS repeats data until it is changed. This function creates a new column which converts repeated values to nulls,
    so we only see newly submitted values once.

    For each workplace, this function iterates over the dataframe in date order and compares the current column value to the
    previously submitted value. If the value differs from the previously submitted value then enter that value into the new column.
    Otherwise null the value in the new column as it is a previously submitted value which has been repeated.

    Args:
        df: The dataframe to use
        column_to_clean: The name of the column to convert
        new_column_name: (optional) If not provided, "_deduplicated" will be appended onto the original column name

    Returns:
        A DataFrame with an addional column with repeated values changed to nulls.
    """
    PREVIOUS_VALUE: str = "previous_value"

    if new_column_name is None:
        new_column_name = column_to_clean + "_deduplicated"

    w = Window.partitionBy(AWPClean.establishment_id).orderBy(
        AWPClean.ascwds_workplace_import_date
    )

    df_with_previously_submitted_value = df.withColumn(
        PREVIOUS_VALUE, F.lag(column_to_clean).over(w)
    )

    df_without_repeated_values = df_with_previously_submitted_value.withColumn(
        new_column_name,
        F.when(
            (F.col(PREVIOUS_VALUE).isNull())
            | (F.col(column_to_clean) != F.col(PREVIOUS_VALUE)),
            F.col(column_to_clean),
        ).otherwise(None),
    )

    df_without_repeated_values = df_without_repeated_values.drop(PREVIOUS_VALUE)

    return df_without_repeated_values


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
