from datetime import date
import sys

import pyspark.sql
import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType, StringType
from pyspark.sql import SparkSession

from utils import utils
from utils.estimate_job_count.column_names import (
    LOCATION_ID,
    SERVICES_OFFERED,
    PEOPLE_DIRECTLY_EMPLOYED,
    NUMBER_OF_BEDS,
    SNAPSHOT_DATE,
    JOB_COUNT_UNFILTERED,
    JOB_COUNT_UNFILTERED_SOURCE,
    JOB_COUNT,
    LOCAL_AUTHORITY,
    REGISTRATION_STATUS,
    ESTIMATE_JOB_COUNT,
    ESTIMATE_JOB_COUNT_SOURCE,
    PRIMARY_SERVICE_TYPE,
    CQC_SECTOR,
)
from utils.estimate_job_count.models.care_homes import model_care_homes
from utils.estimate_job_count.models.extrapolation import model_extrapolation
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
    prepared_locations_cleaned_source,
    carehome_features_source,
    nonres_features_source,
    destination,
    care_home_model_directory,
    non_res_model_directory,
    metrics_destination,
    job_run_id,
    job_name,
):
    spark = (
        SparkSession.builder.appName("sfc_data_engineering_estimate_jobs")
        .config("spark.sql.broadcastTimeout", 600)
        .getOrCreate()
    )
    print("Estimating job counts")

    locations_df = (
        spark.read.parquet(prepared_locations_cleaned_source)
        .select(
            LOCATION_ID,
            SERVICES_OFFERED,
            PRIMARY_SERVICE_TYPE,
            PEOPLE_DIRECTLY_EMPLOYED,
            NUMBER_OF_BEDS,
            SNAPSHOT_DATE,
            JOB_COUNT_UNFILTERED,
            JOB_COUNT_UNFILTERED_SOURCE,
            JOB_COUNT,
            LOCAL_AUTHORITY,
            CQC_SECTOR,
        )
        .filter(f"{REGISTRATION_STATUS} = 'Registered'")
    )

    # loads model features
    carehome_features_df = spark.read.parquet(carehome_features_source)
    non_res_features_df = spark.read.parquet(nonres_features_source)

    locations_df = filter_to_only_cqc_independent_sector_data(locations_df)

    locations_df = locations_df.withColumn(
        ESTIMATE_JOB_COUNT, F.lit(None).cast(IntegerType())
    )
    locations_df = locations_df.withColumn(
        ESTIMATE_JOB_COUNT_SOURCE, F.lit(None).cast(StringType())
    )
    latest_snapshot = utils.get_max_snapshot_date(locations_df)

    locations_df = populate_estimate_jobs_when_job_count_known(locations_df)

    locations_df = model_extrapolation(locations_df)

    # Care homes model
    locations_df, care_home_metrics_info = model_care_homes(
        locations_df,
        carehome_features_df,
        care_home_model_directory,
    )

    care_home_model_info = care_home_model_directory.split("/")
    write_metrics_df(
        metrics_destination,
        r2=care_home_metrics_info["r2"],
        data_percentage=care_home_metrics_info["data_percentage"],
        model_version=care_home_model_info[-2],
        model_name="care_home_with_nursing_historical_jobs_prediction",
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

    non_res_model_info = non_res_model_directory.split("/")
    write_metrics_df(
        metrics_destination,
        r2=non_residential_with_pir_metrics_info["r2"],
        data_percentage=non_residential_with_pir_metrics_info["data_percentage"],
        model_version=non_res_model_info[-2],
        model_name="non_residential_with_pir",
        latest_snapshot=latest_snapshot,
        job_run_id=job_run_id,
        job_name=job_name,
    )

    locations_df = locations_df.withColumnRenamed(
        "rolling_average", "rolling_average_model"
    )
    locations_df = locations_df.withColumn(
        ESTIMATE_JOB_COUNT,
        F.when(
            F.col(ESTIMATE_JOB_COUNT).isNotNull(), F.col(ESTIMATE_JOB_COUNT)
        ).otherwise(F.col("rolling_average_model")),
    )
    locations_df = update_dataframe_with_identifying_rule(
        locations_df, "rolling_average_model", ESTIMATE_JOB_COUNT
    )

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


def populate_estimate_jobs_when_job_count_known(
    df: pyspark.sql.DataFrame,
) -> pyspark.sql.DataFrame:
    df = df.withColumn(
        ESTIMATE_JOB_COUNT,
        F.when(
            (F.col(ESTIMATE_JOB_COUNT).isNull() & (F.col(JOB_COUNT).isNotNull())),
            F.col(JOB_COUNT),
        ).otherwise(F.col(ESTIMATE_JOB_COUNT)),
    )

    df = update_dataframe_with_identifying_rule(
        df, "ascwds_job_count", ESTIMATE_JOB_COUNT
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
        prepared_locations_cleaned_source,
        carehome_features_source,
        nonres_features_source,
        destination,
        care_home_model_directory,
        non_res_with_pir_model_directory,
        metrics_destination,
        JOB_RUN_ID,
        JOB_NAME,
    ) = utils.collect_arguments(
        (
            "--prepared_locations_cleaned_source",
            "Source s3 directory for prepared_locations_cleaned",
        ),
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
        prepared_locations_cleaned_source,
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
