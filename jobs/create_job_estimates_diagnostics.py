from datetime import datetime, date
import sys

import pyspark.sql.functions as F
from pyspark.sql.types import StringType
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame

from utils import utils

from utils.diagnostics_utils.diagnostics_meta_data import (
    Variables as Values,
    Prefixes,
    CareWorkerToJobsRatio as Ratio,
    ResidualsRequired,
)
from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCQC,
)
from utils.column_names.capacity_tracker_columns import CapacityTrackerColumns as CT
from utils.column_values.categorical_column_values import CareHome, DataSource

estimate_filled_posts_columns: list = [
    IndCQC.location_id,
    IndCQC.cqc_location_import_date,
    IndCQC.ascwds_filled_posts_dedup_clean,
    IndCQC.primary_service_type,
    IndCQC.rolling_average_model,
    IndCQC.care_home_model,
    IndCQC.extrapolation_care_home_model,
    IndCQC.interpolation_model,
    IndCQC.non_res_model,
    IndCQC.estimate_filled_posts,
    IndCQC.people_directly_employed,
]
capacity_tracker_care_home_columns: list = [
    CT.cqc_id,
    CT.nurses_employed,
    CT.care_workers_employed,
    CT.non_care_workers_employed,
    CT.agency_nurses_employed,
    CT.agency_care_workers_employed,
    CT.agency_non_care_workers_employed,
]
capacity_tracker_non_residential_columns: list = [
    CT.cqc_id,
    CT.cqc_care_workers_employed,
]


def main(
    estimate_job_counts_source,
    capacity_tracker_care_home_source,
    capacity_tracker_non_residential_source,
    diagnostics_destination,
    residuals_destination,
    description_of_change,
):
    spark = utils.get_spark()
    print("Creating diagnostics for job estimates")

    now = datetime.now()
    run_timestamp = now.strftime("%Y-%m-%d, %H:%M:%S")

    job_estimates_df: DataFrame = utils.read_from_parquet(
        estimate_job_counts_source, estimate_filled_posts_columns
    )

    capacity_tracker_care_homes_df: DataFrame = utils.read_from_parquet(
        capacity_tracker_care_home_source, capacity_tracker_care_home_columns
    )

    capacity_tracker_non_residential_df: DataFrame = utils.read_from_parquet(
        capacity_tracker_non_residential_source,
        capacity_tracker_non_residential_columns,
    )

    capacity_tracker_care_homes_df = prepare_capacity_tracker_care_home_data(
        capacity_tracker_care_homes_df
    )
    capacity_tracker_non_residential_df = prepare_capacity_tracker_non_residential_data(
        capacity_tracker_non_residential_df
    )

    diagnostics_df = merge_dataframes(
        job_estimates_df,
        capacity_tracker_care_homes_df,
        capacity_tracker_non_residential_df,
    )

    diagnostics_prepared_df = diagnostics_df.select(
        IndCQC.location_id,
        IndCQC.cqc_location_import_date,
        IndCQC.ascwds_filled_posts_dedup_clean,
        IndCQC.primary_service_type,
        IndCQC.rolling_average_model,
        IndCQC.care_home_model,
        IndCQC.extrapolation_care_home_model,
        IndCQC.interpolation_model,
        IndCQC.non_res_model,
        IndCQC.estimate_filled_posts,
        IndCQC.people_directly_employed,
        CT.care_home_employed,
        CT.non_residential_employed,
    )

    residuals_list: list = create_residuals_list(
        ResidualsRequired.models,
        ResidualsRequired.services,
        ResidualsRequired.data_source_columns,
    )
    column_names_list = create_column_names_list(residuals_list)
    residuals_df = run_residuals(diagnostics_prepared_df, residuals_list)
    residuals_df = add_timestamp_column(residuals_df, run_timestamp)

    today = date.today()
    residuals_df = residuals_df.withColumn("run_year", F.lit(today.year))
    residuals_df = residuals_df.withColumn("run_month", F.lit(f"{today.month:0>2}"))
    residuals_df = residuals_df.withColumn("run_day", F.lit(f"{today.day:0>2}"))

    utils.write_to_parquet(
        residuals_df,
        residuals_destination,
        mode="append",
        partitionKeys=["run_year", "run_month", "run_day"],
    )

    average_residuals_df = create_empty_dataframe(description_of_change, spark)

    average_residuals_df = run_average_residuals(
        residuals_df, average_residuals_df, column_names_list
    )
    average_residuals_df = add_timestamp_column(average_residuals_df, run_timestamp)

    average_residuals_df = average_residuals_df.withColumn(
        "run_year", F.lit(today.year)
    )
    average_residuals_df = average_residuals_df.withColumn(
        "run_month", F.lit(f"{today.month:0>2}")
    )
    average_residuals_df = average_residuals_df.withColumn(
        "run_day", F.lit(f"{today.day:0>2}")
    )

    utils.write_to_parquet(
        average_residuals_df,
        diagnostics_destination,
        mode="append",
        partitionKeys=["run_year", "run_month", "run_day"],
    )


def add_snapshot_date_to_capacity_tracker_dataframe(
    df: DataFrame, column_name: str
) -> DataFrame:
    df = df.withColumn(column_name, F.lit(Values.capacity_tracker_snapshot_date))
    df = df.withColumn(column_name, F.to_date(column_name, "yyyyMMdd"))
    return df


def merge_dataframes(
    job_estimates_df: DataFrame,
    capacity_tracker_care_homes_df: DataFrame,
    capacity_tracker_non_residential_df: DataFrame,
) -> DataFrame:
    capacity_tracker_care_homes_df_with_snapshot_date = (
        add_snapshot_date_to_capacity_tracker_dataframe(
            capacity_tracker_care_homes_df,
            CT.capacity_tracker_care_homes_snapshot_date,
        )
    )
    capacity_tracker_non_residential_df_with_snapshot_date = (
        add_snapshot_date_to_capacity_tracker_dataframe(
            capacity_tracker_non_residential_df,
            CT.capacity_tracker_non_residential_snapshot_date,
        )
    )

    diagnostics_df: DataFrame = job_estimates_df.join(
        capacity_tracker_care_homes_df_with_snapshot_date,
        [
            (
                job_estimates_df[IndCQC.location_id]
                == capacity_tracker_care_homes_df_with_snapshot_date[CT.cqc_id]
            ),
            (
                job_estimates_df[IndCQC.cqc_location_import_date]
                == capacity_tracker_care_homes_df_with_snapshot_date[
                    CT.capacity_tracker_care_homes_snapshot_date
                ]
            ),
        ],
        how="left",
    )
    diagnostics_df = diagnostics_df.join(
        capacity_tracker_non_residential_df_with_snapshot_date,
        [
            diagnostics_df[IndCQC.location_id]
            == capacity_tracker_non_residential_df_with_snapshot_date[CT.cqc_id],
            (
                job_estimates_df[IndCQC.cqc_location_import_date]
                == capacity_tracker_non_residential_df_with_snapshot_date[
                    CT.capacity_tracker_non_residential_snapshot_date
                ]
            ),
        ],
        how="left",
    )
    return diagnostics_df


def prepare_capacity_tracker_care_home_data(diagnostics_df: DataFrame) -> DataFrame:
    diagnostics_df = diagnostics_df.withColumn(
        CT.care_home_employed,
        (
            diagnostics_df[CT.nurses_employed]
            + diagnostics_df[CT.care_workers_employed]
            + diagnostics_df[CT.non_care_workers_employed]
            + diagnostics_df[CT.agency_nurses_employed]
            + diagnostics_df[CT.agency_care_workers_employed]
            + diagnostics_df[CT.agency_non_care_workers_employed]
        ),
    )
    return diagnostics_df


def prepare_capacity_tracker_non_residential_data(
    diagnostics_df: DataFrame,
) -> DataFrame:
    diagnostics_df = diagnostics_df.withColumn(
        CT.non_residential_employed,
        (
            diagnostics_df[CT.cqc_care_workers_employed]
            * Ratio.care_worker_to_all_jobs_ratio
        ),
    )
    return diagnostics_df


def create_residuals_column_name(
    model: str, service: str, data_source_column: str
) -> str:
    if (data_source_column == CT.care_home_employed) | (
        data_source_column == CT.non_residential_employed
    ):
        data_source = DataSource.capacity_tracker
    elif data_source_column == IndCQC.people_directly_employed:
        data_source = DataSource.pir
    elif data_source_column == IndCQC.ascwds_filled_posts_dedup_clean:
        data_source = DataSource.asc_wds

    if service == CareHome.care_home:
        service_name = "care_home"
    else:
        service_name = "non_res"

    new_column_name = f"{Prefixes.residuals}{model}_{service_name}_{data_source}"
    return new_column_name


def calculate_residuals(
    df: DataFrame,
    model: str,
    service: str,
    data_source_column: str,
) -> (DataFrame, str):
    new_column_name = create_residuals_column_name(model, service, data_source_column)
    df_with_residuals_column = df.withColumn(
        new_column_name, df[model] - df[data_source_column]
    )
    return df_with_residuals_column


def create_residuals_list(
    models: list, services: list, data_source_columns: list
) -> list:
    residuals_list = []

    for model in models:
        for service in services:
            for data_source_column in data_source_columns:
                combination = [model, service, data_source_column]
                residuals_list.append(combination)

    for combination in residuals_list[:]:
        if (
            (combination[1] == CareHome.care_home)
            & (combination[2] == CT.non_residential_employed)
        ) | (
            (combination[1] == CareHome.not_care_home)
            & (combination[2] == CT.care_home_employed)
        ):
            residuals_list.remove(combination)

    return residuals_list


def create_column_names_list(residuals_list: list) -> list:
    column_names_list = []
    for combination in residuals_list:
        column_name = create_residuals_column_name(
            combination[0], combination[1], combination[2]
        )
        column_names_list.append(column_name)
    return column_names_list


def run_residuals(df: DataFrame, residuals_list: list) -> (DataFrame, list):
    for combination in residuals_list:
        df = calculate_residuals(df, combination[0], combination[1], combination[2])
    return df


def calculate_average_residual(
    df: DataFrame, residual_column_name: str, average_residual_column_name: str
) -> DataFrame:
    average_residual_df = df.agg(
        F.avg(df[residual_column_name]).alias(average_residual_column_name)
    )
    return average_residual_df


def run_average_residuals(
    df: DataFrame, average_residuals_df: DataFrame, residuals_columns: list
) -> DataFrame:
    for column in residuals_columns:
        average_residual_column_name = Prefixes.avg + column
        average_df = calculate_average_residual(
            df, column, average_residual_column_name
        )
        average_df = average_df.withColumn(CT.id, F.lit("A"))
        average_residuals_df = average_residuals_df.withColumn(CT.id, F.lit("A"))
        average_residuals_df = average_residuals_df.join(average_df, on=[CT.id]).drop(
            CT.id
        )
    return average_residuals_df


def create_empty_dataframe(
    description_of_change: str, spark: SparkSession
) -> DataFrame:
    column = CT.description_of_changes
    rows = [(description_of_change)]
    df: DataFrame = spark.createDataFrame(rows, StringType())
    df = df.withColumnRenamed(CT.value, column)
    return df


def add_timestamp_column(df: DataFrame, run_timestamp: str) -> DataFrame:
    df = df.withColumn(CT.run_timestamp, F.lit(run_timestamp))
    return df


if __name__ == "__main__":
    print("Spark job 'create_estimate_job_counts_diagnostics' starting...")
    print(f"Job parameters: {sys.argv}")

    (
        estimate_job_counts_source,
        capacity_tracker_care_home_source,
        capacity_tracker_non_residential_source,
        diagnostics_destination,
        residuals_destination,
        description_of_change,
    ) = utils.collect_arguments(
        (
            "--estimate_job_counts_source",
            "Source s3 directory for job_estimates",
        ),
        (
            "--capacity_tracker_care_home_source",
            "Source s3 directory for capacity tracker care home data",
        ),
        (
            "--capacity_tracker_non_residential_source",
            "Source s3 directory for capacity tracker non residential data",
        ),
        (
            "--diagnostics_destination",
            "A destination directory for outputting summary diagnostics tables.",
        ),
        (
            "--residuals_destination",
            "A destination directory for outputting detailed residuals tables with which to make histograms.",
        ),
        (
            "--description_of_change",
            "A description of the changes in the pipeline that will affect these diagnostics.",
        ),
    )

    main(
        estimate_job_counts_source,
        capacity_tracker_care_home_source,
        capacity_tracker_non_residential_source,
        diagnostics_destination,
        residuals_destination,
        description_of_change,
    )

    print("Spark job 'create_estimate_job_counts_diagnostics' complete")
