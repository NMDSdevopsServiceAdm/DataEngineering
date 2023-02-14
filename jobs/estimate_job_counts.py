from datetime import date
import sys

import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType, StringType
from pyspark.ml.regression import GBTRegressionModel
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql import Window

from utils import utils

from utils.prepare_locations_utils.job_calculator.job_calculator import (
    update_dataframe_with_identifying_rule,
)


# Constant values
NURSING_HOME_IDENTIFIER = "Care home with nursing"
NONE_NURSING_HOME_IDENTIFIER = "Care home without nursing"
NONE_RESIDENTIAL_IDENTIFIER = "non-residential"

# Column names
LOCATION_ID = "locationid"
LAST_KNOWN_JOB_COUNT = "last_known_job_count"
ESTIMATE_JOB_COUNT = "estimate_job_count"
ESTIMATE_JOB_COUNT_SOURCE = ESTIMATE_JOB_COUNT + "_source"
PRIMARY_SERVICE_TYPE = "primary_service_type"
PEOPLE_DIRECTLY_EMPLOYED = "people_directly_employed"
NUMBER_OF_BEDS = "number_of_beds"
REGISTRATION_STATUS = "registration_status"
LOCATION_TYPE = "location_type"
LOCAL_AUTHORITY = "local_authority"
SERVICES_OFFERED = "services_offered"
JOB_COUNT = "job_count"
ASCWDS_IMPORT_DATE = "ascwds_workplace_import_date"
SNAPSHOT_DATE = "snapshot_date"


def main(
    prepared_locations_source,
    prepared_locations_features,
    destination,
    care_home_model_directory,
    non_res_with_pir_model_directory,
    metrics_destination,
    job_run_id,
    job_name,
):
    spark = utils.get_spark()
    print("Estimating job counts")

    # load locations_prepared df
    locations_df = (
        spark.read.parquet(prepared_locations_source)
        .select(
            LOCATION_ID,
            SERVICES_OFFERED,
            PEOPLE_DIRECTLY_EMPLOYED,
            NUMBER_OF_BEDS,
            SNAPSHOT_DATE,
            JOB_COUNT,
            LOCAL_AUTHORITY,
        )
        .filter(f"{REGISTRATION_STATUS} = 'Registered'")
    )

    # loads model features
    features_df = spark.read.parquet(prepared_locations_features)

    locations_df = populate_last_known_job_count(locations_df)
    locations_df = locations_df.withColumn(
        ESTIMATE_JOB_COUNT, F.lit(None).cast(IntegerType())
    )
    locations_df = locations_df.withColumn(
        ESTIMATE_JOB_COUNT_SOURCE, F.lit(None).cast(StringType())
    )
    latest_snapshot = utils.get_max_snapshot_date(locations_df)

    locations_df = determine_ascwds_primary_service_type(locations_df)

    # if job_count is populated, add that figure into estimate_job_count column
    locations_df = populate_estimate_jobs_when_job_count_known(locations_df)

    # Care homes model
    latest_care_home_model_version = max(
        utils.get_s3_sub_folders_for_path(care_home_model_directory)
    )
    locations_df, care_home_metrics_info = model_care_homes(
        locations_df,
        features_df,
        f"{care_home_model_directory}{latest_care_home_model_version}/",
    )

    care_home_model_name = utils.get_model_name(care_home_model_directory)
    write_metrics_df(
        metrics_destination,
        r2=care_home_metrics_info["r2"],
        data_percentage=care_home_metrics_info["data_percentage"],
        model_version=latest_care_home_model_version,
        model_name=care_home_model_name,
        latest_snapshot=latest_snapshot,
        job_run_id=job_run_id,
        job_name=job_name,
    )

    # Non-res with PIR data model
    latest_non_res_with_pir_model_version = max(
        utils.get_s3_sub_folders_for_path(non_res_with_pir_model_directory)
    )

    (
        locations_df,
        non_residential_with_pir_metrics_info,
    ) = model_non_residential_with_pir(
        locations_df,
        features_df,
        f"{non_res_with_pir_model_directory}{latest_non_res_with_pir_model_version}/",
    )

    non_residential_with_pir_model_name = utils.get_model_name(
        non_res_with_pir_model_directory
    )
    write_metrics_df(
        metrics_destination,
        r2=non_residential_with_pir_metrics_info["r2"],
        data_percentage=non_residential_with_pir_metrics_info["data_percentage"],
        model_version=latest_non_res_with_pir_model_version,
        model_name=non_residential_with_pir_model_name,
        latest_snapshot=latest_snapshot,
        job_run_id=job_run_id,
        job_name=job_name,
    )

    # Non-res & no PIR data models
    locations_df = model_non_res_historical(locations_df)

    locations_df = model_non_res_default(locations_df)

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


def determine_ascwds_primary_service_type(input_df):
    return input_df.withColumn(
        PRIMARY_SERVICE_TYPE,
        F.when(
            F.array_contains(
                input_df[SERVICES_OFFERED], "Care home service with nursing"
            ),
            NURSING_HOME_IDENTIFIER,
        )
        .when(
            F.array_contains(
                input_df[SERVICES_OFFERED], "Care home service without nursing"
            ),
            NONE_NURSING_HOME_IDENTIFIER,
        )
        .otherwise(NONE_RESIDENTIAL_IDENTIFIER),
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


def model_non_res_historical(df):
    """
    Non-res : Historical :  : 2021 jobs = Last known value *1.03
    """
    # TODO: remove magic number 1.03
    df = df.withColumn(
        ESTIMATE_JOB_COUNT,
        F.when(
            (
                F.col(ESTIMATE_JOB_COUNT).isNull()
                & (F.col(PRIMARY_SERVICE_TYPE) == "non-residential")
                & F.col(LAST_KNOWN_JOB_COUNT).isNotNull()
            ),
            F.col(LAST_KNOWN_JOB_COUNT) * 1.03,
        ).otherwise(F.col(ESTIMATE_JOB_COUNT)),
    )
    df = update_dataframe_with_identifying_rule(
        df,
        "model_non_res_ascwds_projected_forward",
        ESTIMATE_JOB_COUNT,
    )

    return df


def model_non_res_default(df):
    """
    Non-res : Not Historical : Not PIR : 2021 jobs = mean of known 2021 non-res jobs (54.09)
    """
    # TODO: remove magic number 54.09

    df = df.withColumn(
        ESTIMATE_JOB_COUNT,
        F.when(
            (
                F.col(ESTIMATE_JOB_COUNT).isNull()
                & (F.col(PRIMARY_SERVICE_TYPE) == "non-residential")
            ),
            54.09,
        ).otherwise(F.col(ESTIMATE_JOB_COUNT)),
    )
    df = update_dataframe_with_identifying_rule(
        df, "model_non_res_average", ESTIMATE_JOB_COUNT
    )

    return df


def insert_predictions_into_locations(locations_df, predictions_df):
    locations_with_predictions = locations_df.join(
        predictions_df,
        (locations_df["locationid"] == predictions_df["locationid"])
        & (locations_df["snapshot_date"] == predictions_df["snapshot_date"]),
        "left",
    )

    locations_with_predictions = locations_with_predictions.select(
        locations_df["*"], predictions_df["prediction"]
    )

    locations_with_predictions = locations_with_predictions.withColumn(
        ESTIMATE_JOB_COUNT,
        F.when(
            F.col(ESTIMATE_JOB_COUNT).isNotNull(), F.col(ESTIMATE_JOB_COUNT)
        ).otherwise(F.col("prediction")),
    )

    locations_df = locations_with_predictions.drop(F.col("prediction"))
    return locations_df


def model_care_homes(locations_df, features_df, model_path):
    gbt_trained_model = GBTRegressionModel.load(model_path)
    features_df = features_df.where("carehome = 'Y'")
    features_df = features_df.where("ons_region is not null")
    features_df = features_df.where("number_of_beds is not null")

    features_df = features_df.withColumnRenamed("care_home_features", "features")

    care_home_predictions = gbt_trained_model.transform(features_df)

    non_null_job_count_df = care_home_predictions.where("job_count is not null")

    metrics_info = {
        "r2": generate_r2_metric(non_null_job_count_df, "prediction", "job_count"),
        "data_percentage": (features_df.count() / locations_df.count()) * 100,
    }
    locations_df = insert_predictions_into_locations(
        locations_df, care_home_predictions
    )

    locations_df = update_dataframe_with_identifying_rule(
        locations_df, "model_care_homes", ESTIMATE_JOB_COUNT
    )

    return locations_df, metrics_info


def model_non_residential_with_pir(locations_df, features_df, model_path):
    gbt_trained_model = GBTRegressionModel.load(model_path)

    features_df = features_df.where("carehome = 'N'")
    features_df = features_df.where("ons_region is not null")
    features_df = features_df.where("people_directly_employed > 0")

    features_df = features_df.withColumnRenamed(
        "non_residential_inc_pir_features", "features"
    )

    non_residential_with_pir_predictions = gbt_trained_model.transform(features_df)

    non_null_job_count_df = non_residential_with_pir_predictions.where(
        "job_count is not null"
    )

    metrics_info = {
        "r2": generate_r2_metric(non_null_job_count_df, "prediction", "job_count"),
        "data_percentage": (features_df.count() / locations_df.count()) * 100,
    }

    locations_df = insert_predictions_into_locations(
        locations_df, non_residential_with_pir_predictions
    )
    locations_df = update_dataframe_with_identifying_rule(
        locations_df,
        "model_non_res_with_pir",
        ESTIMATE_JOB_COUNT,
    )

    return locations_df, metrics_info


def generate_r2_metric(df, prediction, label):
    model_evaluator = RegressionEvaluator(
        predictionCol=prediction, labelCol=label, metricName="r2"
    )

    r2 = model_evaluator.evaluate(df)
    print("Calculating R Squared (R2) = %g" % r2)

    return r2


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
        prepared_locations_features,
        destination,
        care_home_model_directory,
        non_res_with_pir_model_directory,
        metrics_destination,
        JOB_RUN_ID,
        JOB_NAME,
    ) = utils.collect_arguments(
        ("--prepared_locations_source", "Source s3 directory for prepared_locations"),
        (
            "--prepared_locations_features",
            "Source s3 directory for prepared_locations ML features",
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
        prepared_locations_features,
        destination,
        care_home_model_directory,
        non_res_with_pir_model_directory,
        metrics_destination,
        JOB_RUN_ID,
        JOB_NAME,
    )

    print("Spark job 'estimate_job_counts' complete")
