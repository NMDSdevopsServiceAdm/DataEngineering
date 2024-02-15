import sys

from pyspark.sql import DataFrame
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


def main(source: str, destination: str):
    ascwds_workplace_df = utils.read_from_parquet(source)

    ascwds_workplace_df = cUtils.column_to_date(
        ascwds_workplace_df,
        PartitionKeys.import_date,
        AWPClean.ascwds_workplace_import_date,
    )

    ascwds_workplace_df = cast_to_int(
        ascwds_workplace_df, [AWP.total_staff, AWP.worker_records]
    )

    ascwds_workplace_df = add_purge_outdated_workplaces_column(
        ascwds_workplace_df, AWPClean.ascwds_workplace_import_date
    )

    print(f"Exporting as parquet to {destination}")
    utils.write_to_parquet(
        ascwds_workplace_df,
        destination,
        True,
        [
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
