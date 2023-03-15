from datetime import date
import sys

import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType, StringType
from pyspark.sql import Window, SparkSession

from utils import utils
from utils.estimate_job_count.column_names import (
    LOCATION_ID,
    SERVICES_OFFERED,
    PEOPLE_DIRECTLY_EMPLOYED,
    NUMBER_OF_BEDS,
    SNAPSHOT_DATE,
    JOB_COUNT,
    JOB_COUNT_SOURCE,
    LOCAL_AUTHORITY,
    REGISTRATION_STATUS,
    ESTIMATE_JOB_COUNT,
    ESTIMATE_JOB_COUNT_SOURCE,
    PRIMARY_SERVICE_TYPE,
    LAST_KNOWN_JOB_COUNT,
    CQC_SECTOR,
)
from utils.estimate_job_count.models.care_homes import model_care_homes
from utils.estimate_job_count.models.non_res_rolling_average import (
    model_non_res_rolling_average,
)

from utils.estimate_job_count.models.non_res_historical import (
    model_non_res_historical,
)
from utils.estimate_job_count.models.non_res_with_pir import (
    model_non_residential_with_pir,
)

from utils.prepare_locations_utils.job_calculator.job_calculator import (
    update_dataframe_with_identifying_rule,
)
from utils.estimate_job_count.common_filtering_functions import (
    filter_to_only_cqc_independent_sector_data,
)


def main(
    prepared_locations_source,
    carehome_features_source,
    nonres_features_source,
    destination,
    care_home_model_directory,
    non_res_model_directory,
    metrics_destination,
    job_run_id,
    job_name,
):
    spark = SparkSession.builder.appName("sfc_data_engineering_estimate_jobs").config("spark.sql.broadcastTimeout", 600).getOrCreate()
    print("Estimating job counts")

    # load locations_prepared df
    locations_df = (
        spark.read.parquet(prepared_locations_source)
        .select(
            LOCATION_ID,
            SERVICES_OFFERED,
            PRIMARY_SERVICE_TYPE,
            PEOPLE_DIRECTLY_EMPLOYED,
            NUMBER_OF_BEDS,
            SNAPSHOT_DATE,
            JOB_COUNT,
            JOB_COUNT_SOURCE,
            LOCAL_AUTHORITY,
            CQC_SECTOR,
        )
        .filter(f"{REGISTRATION_STATUS} = 'Registered'")
    )

    locations_df = filter_to_only_cqc_independent_sector_data(locations_df)

    # loads model features
    carehome_features_df = spark.read.parquet(carehome_features_source)
    non_res_features_df = spark.read.parquet(nonres_features_source)

    locations_df = populate_last_known_job_count(locations_df)
    locations_df = locations_df.withColumn(
        ESTIMATE_JOB_COUNT, F.lit(None).cast(IntegerType())
    )
    locations_df = locations_df.withColumn(
        ESTIMATE_JOB_COUNT_SOURCE, F.lit(None).cast(StringType())
    )
    latest_snapshot = utils.get_max_snapshot_date(locations_df)

    # if job_count is populated, add that figure into estimate_job_count column
    locations_df = populate_estimate_jobs_when_job_count_known(locations_df)

    # Care homes model
    locations_df, care_home_metrics_info = model_care_homes(
        locations_df,
        carehome_features_df,
        care_home_model_directory,
    )

    care_home_model_name = utils.get_model_name(care_home_model_directory)
    write_metrics_df(
        metrics_destination,
        r2=care_home_metrics_info["r2"],
        data_percentage=care_home_metrics_info["data_percentage"],
        model_version="2.0.0",
        model_name="ZZZ_care_home_with_nursing_historical_jobs_prediction",
        latest_snapshot=latest_snapshot,
        job_run_id=job_run_id,
        job_name=job_name,
    )

    # Non-res with PIR data model
    (
        locations_df,
        non_residential_with_pir_metrics_info,
    ) = model_non_residential_with_pir(
        locations_df,
        non_res_features_df,
        non_res_model_directory,
    )

    write_metrics_df(
        metrics_destination,
        r2=non_residential_with_pir_metrics_info["r2"],
        data_percentage=non_residential_with_pir_metrics_info["data_percentage"],
        model_version="2.0.0",
        model_name="YYY_non_residential_with_pir_jobs_prediction",
        latest_snapshot=latest_snapshot,
        job_run_id=job_run_id,
        job_name=job_name,
    )

    # Non-res & no PIR data models
    locations_df = model_non_res_historical(locations_df)

    locations_df = model_non_res_rolling_average(locations_df)

    today = date.today()
    locations_df = locations_df.withColumn("run_year", F.lit(today.year))
    locations_df = locations_df.withColumn("run_month", F.lit(f"{today.month:0>2}"))
    locations_df = locations_df.withColumn("run_day", F.lit(f"{today.day:0>2}"))

    print("Completed estimated job counts")
    print(f"Exporting as parquet to {destination}")

    utils.write_to_parquet(
        locations_df,
        destination,
        append=True,
        partitionKeys=["run_year", "run_month", "run_day"],
    )


def populate_estimate_jobs_when_job_count_known(df):
    df = df.withColumn(
        ESTIMATE_JOB_COUNT,
        F.when(
            (F.col(ESTIMATE_JOB_COUNT).isNull() & (F.col("job_count").isNotNull())),
            F.col("job_count"),
        ).otherwise(F.col(ESTIMATE_JOB_COUNT)),
    )

    df = update_dataframe_with_identifying_rule(
        df, "ascwds_job_count", ESTIMATE_JOB_COUNT
    )

    return df

    # adds in a previously submitted ASCWDS figure and performs checks:
    # checks to see if current.locationid is exactly equal to previous.locationid AND
    # current snapshot_date is equal to or greater than previous.snapshot_date AND
    # previouus job count is not null. if all pass join


def populate_last_known_job_count(df):
    column_names = df.columns
    df = df.alias("current").join(
        df.alias("previous"),
        (F.col("current.locationid") == F.col("previous.locationid"))
        & (F.col("current.snapshot_date") >= F.col("previous.snapshot_date"))
        & (F.col("previous.job_count").isNotNull()),
        "leftouter",
    )
    locationAndSnapshotPartition = Window.partitionBy(
        "current.locationid", "current.snapshot_date"
    )
    df = df.withColumn(
        "max_date_with_job_count",
        F.max("previous.snapshot_date").over(locationAndSnapshotPartition),
    )

    df = df.where(
        F.col("max_date_with_job_count").isNull()
        | (F.col("max_date_with_job_count") == F.col("previous.snapshot_date"))
    )
    df = df.drop("max_date_with_job_count")

    df = df.withColumn(LAST_KNOWN_JOB_COUNT, F.col("previous.job_count"))
    df = df.select(
        [f"current.{col_name}" for col_name in column_names] + [LAST_KNOWN_JOB_COUNT]
    )

    return df


def write_metrics_df(
    metrics_destination,
    r2,
    data_percentage,
    model_name,
    model_version,
    latest_snapshot,
    job_run_id,
    job_name,
):
    spark = utils.get_spark()
    columns = [
        "r2",
        "percentage_data",
        "latest_snapshot",
        "job_run_id",
        "job_name",
        "model_name",
        "model_version",
    ]
    row = [
        (
            r2,
            data_percentage,
            latest_snapshot,
            job_run_id,
            job_name,
            model_name,
            model_version,
        )
    ]
    df = spark.createDataFrame(row, columns)
    df = df.withColumn("generated_metric_date", F.current_timestamp())

    print(f"Writing model metrics as parquet to {metrics_destination}")
    utils.write_to_parquet(
        df,
        metrics_destination,
        append=True,
        partitionKeys=["model_name", "model_version"],
    )


if __name__ == "__main__":
    print("Spark job 'estimate_job_counts' starting...")
    print(f"Job parameters: {sys.argv}")

    (
        prepared_locations_source,
        carehome_features_source,
        nonres_features_source,
        destination,
        care_home_model_directory,
        non_res_with_pir_model_directory,
        metrics_destination,
        JOB_RUN_ID,
        JOB_NAME,
    ) = utils.collect_arguments(
        ("--prepared_locations_source", "Source s3 directory for prepared_locations"),
        (
            "--carehome_features_source",
            "Source s3 directory for prepared_locations ML features for care homes",
        ),
        (
            "--nonres_features_source",
            "Source s3 directory for prepared_locations ML features for non res care homes",
        ),
        (
            "--destination",
            "A destination directory for outputting cqc locations, if not provided shall default to S3 todays date.",
        ),
        (
            "--care_home_model_directory",
            "The directory where the care home models are saved",
        ),
        (
            "--non_res_with_pir_model_directory",
            "The directory where the non residential with PIR data models are saved",
        ),
        ("--metrics_destination", "The destination for the R2 metric data"),
        ("--JOB_RUN_ID", "The Glue job run id"),
        ("--JOB_NAME", "The Glue job name"),
    )

    main(
        prepared_locations_source,
        carehome_features_source,
        nonres_features_source,
        destination,
        care_home_model_directory,
        non_res_with_pir_model_directory,
        metrics_destination,
        JOB_RUN_ID,
        JOB_NAME,
    )

    print("Spark job 'estimate_job_counts' complete")
