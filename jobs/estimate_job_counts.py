import argparse

import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType
from pyspark.ml.regression import GBTRegressionModel
from pyspark.sql import Window
from utils import utils
from datetime import datetime


# Constant values
NURSING_HOME_IDENTIFIER = "Care home with nursing"
NONE_NURSING_HOME_IDENTIFIER = "Care home without nursing"
NONE_RESIDENTIAL_IDENTIFIER = "non-residential"

# Column names
LOCATION_ID = "locationid"
LAST_KNOWN_JOB_COUNT = "last_known_job_count"
ESTIMATE_JOB_COUNT = "estimate_job_count"
PRIMARY_SERVICE_TYPE = "primary_service_type"
PIR_SERVICE_USERS = "pir_service_users"
NUMBER_OF_BEDS = "number_of_beds"
REGISTRATION_STATUS = "registration_status"
LOCATION_TYPE = "location_type"
SERVICES_OFFERED = "services_offered"
JOB_COUNT = "job_count"
ASCWDS_IMPORT_DATE = "ascwds_workplace_import_date"
SNAPSHOT_DATE = "snapshot_date"


def main(
    prepared_locations_source,
    prepared_locations_features,
    destination,
    care_home_model_directory,
):
    spark = utils.get_spark()
    print("Estimating job counts")
    locations_df = (
        spark.read.parquet(prepared_locations_source)
        .select(
            LOCATION_ID,
            SERVICES_OFFERED,
            PIR_SERVICE_USERS,
            NUMBER_OF_BEDS,
            SNAPSHOT_DATE,
            JOB_COUNT,
        )
        .filter(f"{REGISTRATION_STATUS} = 'Registered'")
    )

    features_df = spark.read.parquet(prepared_locations_features)

    locations_df = populate_last_known_job_count(locations_df)
    locations_df = locations_df.withColumn(
        ESTIMATE_JOB_COUNT, F.lit(None).cast(IntegerType())
    )

    locations_df = determine_ascwds_primary_service_type(locations_df)

    locations_df = populate_estimate_jobs_when_job_count_known(locations_df)
    # Non-res models
    locations_df = model_non_res_historical(locations_df)
    locations_df = model_non_res_historical_pir(locations_df)
    locations_df = model_non_res_default(locations_df)

    # Care homes with historical model
    latest_model_version = max(
        utils.get_s3_sub_folders_for_path(care_home_model_directory)
    )
    locations_df = model_care_home_with_historical(
        locations_df,
        features_df,
        f"{care_home_model_directory}{latest_model_version}/",
    )

    # Nursing models
    locations_df = model_care_home_with_nursing_pir_and_cqc_beds(locations_df)
    locations_df = model_care_home_with_nursing_cqc_beds(locations_df)

    # Non-nursing models
    locations_df = model_care_home_without_nursing_cqc_beds_and_pir(locations_df)
    locations_df = model_care_home_without_nursing_cqc_beds(locations_df)

    today = datetime.now()
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

    return df


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

    return df


def model_non_res_historical_pir(df):
    """
    Non-res : Not Historical : PIR : 2021 jobs = 25.046 + 0.469 * PIR service users
    """
    # TODO: remove magic number 25.046
    # TODO: remove magic number 0.469

    df = df.withColumn(
        ESTIMATE_JOB_COUNT,
        F.when(
            (
                F.col(ESTIMATE_JOB_COUNT).isNull()
                & (F.col(PRIMARY_SERVICE_TYPE) == "non-residential")
                & F.col(PIR_SERVICE_USERS).isNotNull()
            ),
            (25.046 + (0.469 * F.col(PIR_SERVICE_USERS))),
        ).otherwise(F.col(ESTIMATE_JOB_COUNT)),
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


def model_care_home_with_historical(locations_df, features_df, model_path):
    gbt_trained_model = GBTRegressionModel.load(model_path)

    features_df = features_df.where("carehome = 'Y'")
    features_df = features_df.where("region is not null")
    features_df = features_df.where("number_of_beds is not null")

    care_home_predictions = gbt_trained_model.transform(features_df)

    locations_df = insert_predictions_into_locations(
        locations_df, care_home_predictions
    )

    return locations_df


def model_care_home_with_nursing_pir_and_cqc_beds(df):
    """
    Care home with nursing : Not Historical : PIR :  2021 jobs = (0.773*beds)+(0.551*PIR)+0.304
    """
    # TODO: remove magic number 0.773
    # TODO: remove magic number 0.551
    # TODO: remove magic number 0.304

    df = df.withColumn(
        ESTIMATE_JOB_COUNT,
        F.when(
            (
                F.col(ESTIMATE_JOB_COUNT).isNull()
                & (F.col(PRIMARY_SERVICE_TYPE) == "Care home with nursing")
                & F.col(PIR_SERVICE_USERS).isNotNull()
                & F.col(NUMBER_OF_BEDS).isNotNull()
            ),
            (
                (0.773 * F.col(NUMBER_OF_BEDS))
                + (0.551 * F.col(PIR_SERVICE_USERS))
                + 0.304
            ),
        ).otherwise(F.col(ESTIMATE_JOB_COUNT)),
    )

    return df


def model_care_home_with_nursing_cqc_beds(df):
    """
    Care home with nursing : Not Historical : Not PIR : 2021 jobs = (1.203*beds) +2.39
    """
    # TODO: remove magic number 1.203
    # TODO: remove magic number 2.39

    df = df.withColumn(
        ESTIMATE_JOB_COUNT,
        F.when(
            (
                F.col(ESTIMATE_JOB_COUNT).isNull()
                & (F.col(PRIMARY_SERVICE_TYPE) == "Care home with nursing")
                & F.col(NUMBER_OF_BEDS).isNotNull()
            ),
            (1.203 * F.col(NUMBER_OF_BEDS) + 2.39),
        ).otherwise(F.col(ESTIMATE_JOB_COUNT)),
    )

    return df


def model_care_home_without_nursing_cqc_beds_and_pir(df):
    """
    Care home without nursing : Not Historical : PIR :  2021 jobs = 10.652+(0.571*beds)+(0.296*PIR)
    """
    # TODO: remove magic number 10.652
    # TODO: remove magic number 0.571
    # TODO: remove magic number 0.296

    df = df.withColumn(
        ESTIMATE_JOB_COUNT,
        F.when(
            (
                F.col(ESTIMATE_JOB_COUNT).isNull()
                & (F.col(PRIMARY_SERVICE_TYPE) == "Care home without nursing")
                & F.col(PIR_SERVICE_USERS).isNotNull()
                & F.col(NUMBER_OF_BEDS).isNotNull()
            ),
            (
                10.652
                + (0.571 * F.col(NUMBER_OF_BEDS))
                + (0.296 * F.col(PIR_SERVICE_USERS))
            ),
        ).otherwise(F.col(ESTIMATE_JOB_COUNT)),
    )

    return df


def model_care_home_without_nursing_cqc_beds(df):
    """
    Care home without nursing : Not Historical : Not PIR : 2021 jobs = 11.291+(0.8126*beds)
    """
    # TODO: remove magic number 11.291
    # TODO: remove magic number 0.8126

    df = df.withColumn(
        ESTIMATE_JOB_COUNT,
        F.when(
            (
                F.col(ESTIMATE_JOB_COUNT).isNull()
                & (F.col(PRIMARY_SERVICE_TYPE) == "Care home without nursing")
                & F.col(NUMBER_OF_BEDS).isNotNull()
            ),
            (11.291 + (0.8126 * F.col(NUMBER_OF_BEDS))),
        ).otherwise(F.col(ESTIMATE_JOB_COUNT)),
    )

    return df


def collect_arguments():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--prepared_locations_source",
        help="Source s3 directory for prepared_locations",
        required=True,
    )
    parser.add_argument(
        "--prepared_locations_features",
        help="Source s3 directory for prepared_locations ML features",
        required=True,
    )
    parser.add_argument(
        "--destination",
        help="A destination directory for outputting cqc locations, if not provided shall default to S3 todays date.",
        required=True,
    )
    parser.add_argument(
        "--care_home_model_directory",
        help="The directory where the care home models are saved",
        required=True,
    )

    args, _ = parser.parse_known_args()

    return (
        args.prepared_locations_source,
        args.prepared_locations_features,
        args.destination,
        args.care_home_model_directory,
    )


if __name__ == "__main__":
    (
        prepared_locations_source,
        prepared_locations_features,
        destination,
        care_home_model_directory,
    ) = collect_arguments()

    main(
        prepared_locations_source,
        prepared_locations_features,
        destination,
        care_home_model_directory,
    )
