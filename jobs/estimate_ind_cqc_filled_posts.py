import sys
from datetime import date

import pyspark.sql
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType,
    StringType,
)
from pyspark.sql import DataFrame

from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import (
    PartitionKeys as Keys,
    IndCqcColumns as IndCqc,
)
from utils.estimate_job_count.models.care_homes import model_care_homes
from utils.estimate_job_count.models.primary_service_rolling_average import (
    model_primary_service_rolling_average,
)
from utils.estimate_job_count.models.extrapolation import model_extrapolation
from utils.estimate_job_count.models.interpolation import model_interpolation
from utils.estimate_job_count.models.non_res_with_pir import (
    model_non_residential_with_pir,
)
# Update this once Gary's PR is in
from utils.prepare_locations_utils.job_calculator.job_calculator import (
    update_dataframe_with_identifying_rule,
)

cleaned_ind_cqc_columns = [
    IndCqc.location_id,
    IndCqc.services_offered,
    IndCqc.primary_service_type,
    IndCqc.people_directly_employed,
    IndCqc.number_of_beds,
    IndCqc.cqc_location_import_date,
    IndCqc.ascwds_filled_posts,
    IndCqc.ascwds_filled_posts_source,
    IndCqc.ascwds_filled_posts_dedup_clean,
    IndCqc.current_cssr,
    IndCqc.cqc_sector,
]

PartitionKeys = [Keys.year, Keys.month, Keys.day, Keys.import_date]
# Note: using 88 as a proxy for 3 months
NUMBER_OF_DAYS_IN_ROLLING_AVERAGE = 88

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

    # This job requires data filtered to registered, cqc independent sector locations. 
    # These filteres are applied earlier in the pipeline.
    cleaned_ind_cqc_df = utils.read_from_parquet(cleaned_ind_cqc_source, cleaned_ind_cqc_columns)
    
    carehome_features_df = utils.read_from_parquet(care_home_features_source)
    non_res_features_df =utils.read_from_parquet(non_res_features_source)

    cleaned_ind_cqc_df = cleaned_ind_cqc_df.withColumn(
        IndCqc.estimate_filled_posts, F.lit(None).cast(IntegerType())
    )
    cleaned_ind_cqc_df = cleaned_ind_cqc_df.withColumn(
        IndCqc.estimate_filled_posts_source, F.lit(None).cast(StringType())
    )
    latest_import_date = get_max_import_date(cleaned_ind_cqc_df)

    cleaned_ind_cqc_df = utils.create_unix_timestamp_variable_from_date_column(
        cleaned_ind_cqc_df,
        date_col=IndCqc.cqc_location_import_date,
        date_format="yyyy-MM-dd",
        new_col_name="unix_time",
    )

    cleaned_ind_cqc_df = populate_estimate_jobs_when_job_count_known(cleaned_ind_cqc_df)

    cleaned_ind_cqc_df = model_primary_service_rolling_average(
        cleaned_ind_cqc_df, NUMBER_OF_DAYS_IN_ROLLING_AVERAGE
    )

    cleaned_ind_cqc_df = model_extrapolation(cleaned_ind_cqc_df)

    # Care homes model
    cleaned_ind_cqc_df, care_home_metrics_info = model_care_homes(
        cleaned_ind_cqc_df,
        carehome_features_df,
        care_home_model_directory,
    )

    cleaned_ind_cqc_df = model_interpolation(cleaned_ind_cqc_df)

    care_home_model_info = care_home_model_directory.split("/")
    write_metrics_df(
        metrics_destination,
        r2=care_home_metrics_info["r2"],
        data_percentage=care_home_metrics_info["data_percentage"],
        model_version=care_home_model_info[-2],
        model_name="care_home_with_nursing_historical_jobs_prediction",
        latest_import_date=latest_import_date,
        job_run_id=job_run_id,
        job_name=job_name,
    )

    # Non-res with PIR data model
    (
        cleaned_ind_cqc_df,
        non_residential_with_pir_metrics_info,
    ) = model_non_residential_with_pir(
        cleaned_ind_cqc_df,
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
        latest_import_date=latest_import_date,
        job_run_id=job_run_id,
        job_name=job_name,
    )

    cleaned_ind_cqc_df = cleaned_ind_cqc_df.withColumnRenamed(
        "rolling_average", "rolling_average_model"
    )
    cleaned_ind_cqc_df = cleaned_ind_cqc_df.withColumn(
        IndCqc.estimate_filled_posts,
        F.when(
            F.col(IndCqc.estimate_filled_posts).isNotNull(), F.col(IndCqc.estimate_filled_posts)
        ).otherwise(F.col("rolling_average_model")),
    )
    cleaned_ind_cqc_df = update_dataframe_with_identifying_rule(
        cleaned_ind_cqc_df, "rolling_average_model", IndCqc.estimate_filled_posts
    )


    print("Completed estimated job counts")

    print(f"Exporting as parquet to {estimated_ind_cqc_destination}")

    utils.write_to_parquet(
        cleaned_ind_cqc_df,
        estimated_ind_cqc_destination,
        mode="overwrite",
        partitionKeys=PartitionKeys,
    )

def get_max_import_date(df:DataFrame, import_date_column:str) -> str:
    date = df.select(F.max(import_date_column).alias("max")).first().max
    date_as_string = date.strftime("%Y%m%d")
    return date_as_string


def populate_estimate_jobs_when_job_count_known(
    df: pyspark.sql.DataFrame,
) -> pyspark.sql.DataFrame:
    df = df.withColumn(
        IndCqc.estimate_filled_posts,
        F.when(
            (F.col(IndCqc.estimate_filled_posts).isNull() & (F.col(IndCqc.ascwds_filled_posts_dedup_clean).isNotNull())),
            F.col(IndCqc.ascwds_filled_posts_dedup_clean),
        ).otherwise(F.col(IndCqc.estimate_filled_posts)),
    )

    df = update_dataframe_with_identifying_rule(
        df, "ascwds_job_count", IndCqc.estimate_filled_posts
    )

    return df


def write_metrics_df(
    metrics_destination,
    r2,
    data_percentage,
    model_name,
    model_version,
    latest_import_date, 
    job_run_id,
    job_name,
):
    spark = utils.get_spark()
    columns = [
        IndCqc.r2,
        IndCqc.percentage_data,
        IndCqc.latest_import_date,
        IndCqc.job_run_id,
        IndCqc.job_name,
        IndCqc.model_name,
        IndCqc.model_version,
    ]
    row = [
        (
            r2,
            data_percentage,
            latest_import_date,
            job_run_id,
            job_name,
            model_name,
            model_version,
        )
    ]
    df = spark.createDataFrame(row, columns)
    df = df.withColumn(IndCqc.metrics_date, F.current_timestamp())
    print(f"Writing model metrics as parquet to {metrics_destination}")
    utils.write_to_parquet(
        df,
        metrics_destination,
        mode="append",
        partitionKeys=[IndCqc.model_name, IndCqc.model_version],
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
