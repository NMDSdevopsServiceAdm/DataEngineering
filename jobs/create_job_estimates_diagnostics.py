from datetime import date
import sys

import pyspark.sql
import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType, StringType
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame


from utils import utils
from utils.estimate_job_count.column_names import (
    LOCATION_ID,
    PEOPLE_DIRECTLY_EMPLOYED,
    SNAPSHOT_DATE,
    JOB_COUNT_UNFILTERED,
    JOB_COUNT,
    ESTIMATE_JOB_COUNT,
    PRIMARY_SERVICE_TYPE,
    ROLLING_AVERAGE_MODEL,
    EXTRAPOLATION_MODEL,
    CARE_HOME_MODEL,
    INTERPOLATION_MODEL,
    NON_RESIDENTIAL_MODEL,
)
from utils.estimate_job_count.capacity_tracker_column_names import (
    CQC_ID,
    NURSES_EMPLOYED,
    CARE_WORKERS_EMPLOYED,
    NON_CARE_WORKERS_EMPLOYED,
    AGENCY_NURSES_EMPLOYED,
    AGENCY_CARE_WORKERS_EMPLOYED,
    AGENCY_NON_CARE_WORKERS_EMPLOYED,
    CQC_CARE_WORKERS_EMPLOYED,
    CARE_HOME_EMPLOYED,
    NON_RESIDENTIAL_EMPLOYED,
    RESIDUAL_CATEGORY,
)
from utils.estimate_job_count.capacity_tracker_column_values import (
    ascwds_known,
    known_externally,
    unknown,
)


def main(
    estimate_job_counts_source,
    capacity_tracker_care_home_source,
    capacity_tracker_non_residential_source,
    diagnostics_destination,
    residuals_destination,
):
    spark = SparkSession.builder.appName(
        "sfc_data_engineering_job_estimate_diagnostics"
    ).getOrCreate()
    print("Creating diagnostics for job estimates")

    job_estimates_df: DataFrame = spark.read.parquet(estimate_job_counts_source).select(
        LOCATION_ID,
        SNAPSHOT_DATE,
        JOB_COUNT_UNFILTERED,
        JOB_COUNT,
        PRIMARY_SERVICE_TYPE,
        ROLLING_AVERAGE_MODEL,
        CARE_HOME_MODEL,
        EXTRAPOLATION_MODEL,
        INTERPOLATION_MODEL,
        NON_RESIDENTIAL_MODEL,
        ESTIMATE_JOB_COUNT,
        PEOPLE_DIRECTLY_EMPLOYED,
    )

    capacity_tracker_care_homes_df: DataFrame = spark.read.parquet(
        capacity_tracker_care_home_source
    ).select(
        CQC_ID,
        NURSES_EMPLOYED,
        CARE_WORKERS_EMPLOYED,
        NON_CARE_WORKERS_EMPLOYED,
        AGENCY_NURSES_EMPLOYED,
        AGENCY_CARE_WORKERS_EMPLOYED,
        AGENCY_NON_CARE_WORKERS_EMPLOYED,
    )

    capacity_tracker_non_residential_df: DataFrame = spark.read.parquet(
        capacity_tracker_non_residential_source
    ).select(
        CQC_ID,
        CQC_CARE_WORKERS_EMPLOYED,
    )

    diagnostics_df = merge_dataframes(
        job_estimates_df,
        capacity_tracker_care_homes_df,
        capacity_tracker_non_residential_df,
    )
    diagnostics_df = prepare_capacity_tracker_care_home_data(diagnostics_df)
    diagnostics_df = prepare_capacity_tracker_non_residential_data(diagnostics_df)
    diagnostics_prepared_df = diagnostics_df.select(
        LOCATION_ID,
        SNAPSHOT_DATE,
        JOB_COUNT_UNFILTERED,
        JOB_COUNT,
        PRIMARY_SERVICE_TYPE,
        ROLLING_AVERAGE_MODEL,
        CARE_HOME_MODEL,
        EXTRAPOLATION_MODEL,
        INTERPOLATION_MODEL,
        NON_RESIDENTIAL_MODEL,
        ESTIMATE_JOB_COUNT,
        PEOPLE_DIRECTLY_EMPLOYED,
        CARE_HOME_EMPLOYED,
        NON_RESIDENTIAL_EMPLOYED,
    )

    diagnostics_prepared_df = add_categorisation_column(diagnostics_prepared_df)

    # Calculate residuals for each model/ service/ known value status
    # add column to split into groups for model/ service / known value
    # calculate residuals wihin each group (window function?)
        # estmiate job count (output)-> care home/ non-res -> ascwds knwon/ extranlly knwon/ unknown -> 6
        # job count -> care home/ non-res -> ascwds known -> 2
        # care home model-> care home -> ascwds knwon/ extranlly knwon/ unknown -> 3
        # non res model-> non-res -> ascwds knwon/ extranlly knwon/ unknown -> 3
        # interpolation model-> care home/ non-res -> ascwds knwon/ extranlly knwon/ unknown -> 6
        # extrapolation model-> care home/ non-res -> ascwds knwon/ extranlly knwon/ unknown -> 6
        # rolling average model-> care home/ non-res -> ascwds knwon/ extranlly knwon/ unknown -> 6
        # total combinations = 32
    # recode the residual category column into ascwds known care home/ ascwds kown non res/ externally known ct care home/ externally known ct non res/ externally known pir care home / externally known pir non res / unknown
    # select columns
    # explode df so row per model and one "output value" column and one "output model"
    # create column with "known" value using residual category to control flow
    
    # calculate difference between known and outputed values in "residuals" column
    # window by partition on residual category column
    # Calculate average residuals

    # Create table for histograms
    # probably just involves dropping some values

    # Save diagnostics to parquet - append with timestamp

    # Save residuals to parquet - append with timestamp


def merge_dataframes(
    job_estimates_df: DataFrame,
    capacity_tracker_care_homes_df: DataFrame,
    capacity_tracker_non_residential_df: DataFrame,
) -> DataFrame:
    diagnostics_df: DataFrame = job_estimates_df.join(
        capacity_tracker_care_homes_df,
        job_estimates_df[LOCATION_ID] == capacity_tracker_care_homes_df[CQC_ID],
        how="left",
    )
    diagnostics_df = diagnostics_df.join(
        capacity_tracker_non_residential_df,
        diagnostics_df[LOCATION_ID] == capacity_tracker_non_residential_df[CQC_ID],
        how="left",
    )
    return diagnostics_df


def prepare_capacity_tracker_care_home_data(diagnostics_df: DataFrame) -> DataFrame:
    diagnostics_df = diagnostics_df.withColumn(
        CARE_HOME_EMPLOYED,
        (
            diagnostics_df[NURSES_EMPLOYED]
            + diagnostics_df[CARE_WORKERS_EMPLOYED]
            + diagnostics_df[NON_CARE_WORKERS_EMPLOYED]
            + diagnostics_df[AGENCY_NURSES_EMPLOYED]
            + diagnostics_df[AGENCY_CARE_WORKERS_EMPLOYED]
            + diagnostics_df[AGENCY_NON_CARE_WORKERS_EMPLOYED]
        ),
    )
    return diagnostics_df


def prepare_capacity_tracker_non_residential_data(
    diagnostics_df: DataFrame,
) -> DataFrame:
    care_worker_to_all_jobs_ratio = 1.3

    diagnostics_df = diagnostics_df.withColumn(
        NON_RESIDENTIAL_EMPLOYED,
        (diagnostics_df[CQC_CARE_WORKERS_EMPLOYED] * care_worker_to_all_jobs_ratio),
    )
    return diagnostics_df


def add_categorisation_column(
    diagnostics_prepared_df: DataFrame,
) -> DataFrame:
    diagnostics_prepared_df = diagnostics_prepared_df.withColumn(
        RESIDUAL_CATEGORY,
        F.when(diagnostics_prepared_df[JOB_COUNT_UNFILTERED].isNotNull(), ascwds_known)
        .when(
            (diagnostics_prepared_df[JOB_COUNT_UNFILTERED].isNull())
            & (diagnostics_prepared_df[CARE_HOME_EMPLOYED].isNull())
            & (diagnostics_prepared_df[NON_RESIDENTIAL_EMPLOYED].isNull())
            & (diagnostics_prepared_df[PEOPLE_DIRECTLY_EMPLOYED].isNull()),
            unknown,
        )
        .otherwise(known_externally),
    )
    return diagnostics_prepared_df


if __name__ == "__main__":
    print("Spark job 'create_estimate_job_counts_diagnostics' starting...")
    print(f"Job parameters: {sys.argv}")

    (
        estimate_job_counts_source,
        capacity_tracker_care_home_source,
        capacity_tracker_non_residential_source,
        diagnostics_destination,
        residuals_destination,
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
    )

    main(
        estimate_job_counts_source,
        capacity_tracker_care_home_source,
        capacity_tracker_non_residential_source,
        diagnostics_destination,
        residuals_destination,
    )

    print("Spark job 'create_estimate_job_counts_diagnostics' complete")
