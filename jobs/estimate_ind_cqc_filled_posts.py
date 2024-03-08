import sys

import pyspark.sql
from pyspark.sql import functions as F

from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import (
    PartitionKeys as Keys,
    IndCqcColumns as IndCqc,
)
# Update this once Gary's PR is in
from utils.prepare_locations_utils.job_calculator.job_calculator import (
    update_dataframe_with_identifying_rule,
)

PartitionKeys = [Keys.year, Keys.month, Keys.day, Keys.import_date]


def main(
    cleaned_ind_cqc_source: str,
    care_home_features_source: str,
    non_res_features_source:str,
    care_home_model_directory:str,
    non_res_model_directory:str,
    metrics_destination:str,
    estimated_ind_cqc_destination:str,
    job_run_id,
    job_name,
) -> pyspark.sql.DataFrame:
    print("Estimating independent CQC filled posts...")

    cleaned_ind_cqc_df = utils.read_from_parquet(cleaned_ind_cqc_source)

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

    locations_df = utils.create_unix_timestamp_variable_from_date_column(
        locations_df,
        date_col=SNAPSHOT_DATE,
        date_format="yyyy-MM-dd",
        new_col_name="unix_time",
    )

    locations_df = populate_estimate_jobs_when_job_count_known(locations_df)

    locations_df = model_primary_service_rolling_average(
        locations_df, NUMBER_OF_DAYS_IN_ROLLING_AVERAGE
    )

    locations_df = model_extrapolation(locations_df)

    # Care homes model
    locations_df, care_home_metrics_info = model_care_homes(
        locations_df,
        carehome_features_df,
        care_home_model_directory,
    )

    locations_df = model_interpolation(locations_df)

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

    print(f"Exporting as parquet to {estimated_ind_cqc_destination}")

    utils.write_to_parquet(
        cleaned_ind_cqc_df,
        estimated_ind_cqc_destination,
        mode="overwrite",
        partitionKeys=PartitionKeys,
    )



def populate_estimate_jobs_when_job_count_known(
    df: pyspark.sql.DataFrame,
) -> pyspark.sql.DataFrame:
    df = df.withColumn(
        IndCqc.estimate_job_count,
        F.when(
            (F.col(IndCqc.estimate_job_count).isNull() & (F.col(IndCqc.ascwds_filled_posts_dedup_clean).isNotNull())),
            F.col(IndCqc.ascwds_filled_posts_dedup_clean),
        ).otherwise(F.col(IndCqc.estimate_job_count)),
    )

    df = update_dataframe_with_identifying_rule(
        df, "ascwds_job_count", IndCqc.estimate_job_count
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
        mode="append",
        partitionKeys=["model_name", "model_version"],
    )

if __name__ == "__main__":
    print("Spark job 'estimate_ind_cqc_filled_posts' starting...")
    print(f"Job parameters: {sys.argv}")

    (
        cleaned_ind_cqc_source,
        care_home_features_source,
        non_res_features_source,
        care_home_model_directory,
        non_res_model_directory,
        metrics_destination,
        estimated_ind_cqc_destination,
        job_run_id,
        job_name,
    ) = utils.collect_arguments(
        (
            "--cleaned_ind_cqc_source",
            "Source s3 directory for cleaned_ind_cqc_filled_posts",
        ),
        (
            "--care_home_features_source",
            "Source s3 directory for ML features for care homes",
        ),
        (
            "--non_res_features_source",
            "Source s3 directory for ML features for non res",
        ),
        (
            "--care_home_model_directory",
            "The directory where the care home models are saved",
        ),
        (
            "--non_res_model_directory",
            "The directory where the non re models are saved",
        ),
        ("--metrics_destination", "The destination for the R2 metric data"),
        (
            "--estimated_ind_cqc_destination",
            "A destination directory for outputting estimates for filled posts",
        ),
        ("--job_run_id", "The Glue job run id"),
        ("--job_name", "The Glue job name"),
    )

    main(
        cleaned_ind_cqc_source,
        care_home_features_source,
        non_res_features_source,
        care_home_model_directory,
        non_res_model_directory,
        metrics_destination,
        estimated_ind_cqc_destination,
        job_run_id,
        job_name,
    )
