import argparse
import datetime

from pyspark.sql.functions import coalesce, col, lit, array_contains, when
from pyspark.sql.types import IntegerType

from environment import constants
from utils import utils


# Constant values
NURSING_HOME_IDENTIFIER = "Care home with nursing"
NONE_NURSING_HOME_IDENTIFIER = "Care home without nursing"
NONE_RESIDENTIAL_IDENTIFIER = "non-residential"

# Column names
LOCATION_ID = "locationid"
LAST_KNOWN_JOB_COUNT = "last_known_job_count"
ESTIMATE_JOB_COUNT_2021 = "estimate_job_count_2021"
PRIMARY_SERVICE_TYPE = "primary_service_type"
PIR_SERVICE_USERS = "pir_service_users"
CQC_NUMBER_OF_BEDS = "number_of_beds"
REGISTRATION_STATUS = "registration_status"
LOCATION_TYPE = "location_type"
SERVICES_OFFERED = "services_offered"
JOB_COUNT = "job_count"
ASCWDS_IMPORT_DATE = "ascwds_workplace_import_date"


def main(prepared_locations_source, destination, ascwds_import_date=datetime.date(30, 11, 2021)):
    spark = utils.get_spark()
    print("Estimating 2021 jobs")
    locations_df = (
        spark.read.parquet(prepared_locations_source)
        .select(LOCATION_ID, SERVICES_OFFERED)
        .distinct()
        .filter(
            f"{REGISTRATION_STATUS} = 'Registered' \
            and {LOCATION_TYPE} = 'Social Care Org' \
            and {ASCWDS_IMPORT_DATE} = {ascwds_import_date}"
        )
    )

    locations_df = locations_df.withColumn(ESTIMATE_JOB_COUNT_2021, lit(None).cast(IntegerType()))
    locations_df = collect_ascwds_historical_job_figures(spark, prepared_locations_source, locations_df)
    locations_df = determine_ascwds_primary_service_type(locations_df)

    locations_df = model_populate_known_2021_jobs(locations_df)
    # Non-res models
    locations_df = model_non_res_historical(locations_df)
    locations_df = model_non_res_historical_pir(locations_df)
    locations_df = model_non_res_default(locations_df)

    # Nursing models
    locations_df = model_care_home_with_nursing_historical(locations_df)
    locations_df = model_care_home_with_nursing_pir_and_cqc_beds(locations_df)
    locations_df = model_care_home_with_nursing_cqc_beds(locations_df)

    # Non-nursing models
    locations_df = model_care_home_without_nursing_historical(locations_df)
    locations_df = model_care_home_without_nursing_cqc_beds_and_pir(locations_df)
    locations_df = model_care_home_without_nursing_cqc_beds(locations_df)

    print("Completed estimated 2021 jobs")
    print(f"Exporting as parquet to {destination}")
    utils.write_to_parquet(locations_df, destination)


def determine_ascwds_primary_service_type(input_df):
    return input_df.withColumn(
        PRIMARY_SERVICE_TYPE,
        when(array_contains(input_df[SERVICES_OFFERED], "Care home service with nursing"), NURSING_HOME_IDENTIFIER)
        .when(
            array_contains(input_df[SERVICES_OFFERED], "Care home service without nursing"),
            NONE_NURSING_HOME_IDENTIFIER,
        )
        .otherwise(NONE_RESIDENTIAL_IDENTIFIER),
    )


def collect_ascwds_historical_job_figures(spark, data_source, input_df):
    spark.read.parquet(data_source).createOrReplaceTempView("temp_locations_prepared")
    for year in ["2021", "2020", "2019"]:

        jobs_previous = spark.sql(
            f"""select
                locationid,
                max(job_count) as job_count_{year}
                from
                    temp_locations_prepared
                where
                    year(ascwds_workplace_import_date) = {year}
                group by year(ascwds_workplace_import_date), locationid
                having max(year(ascwds_workplace_import_date))
            """
        )
        input_df = input_df.join(jobs_previous, LOCATION_ID, "left")

    # Calculate last known jobs previous to 2021
    input_df = input_df.withColumn(LAST_KNOWN_JOB_COUNT, coalesce("job_count_2020", "job_count_2019"))

    return input_df


def model_populate_known_2021_jobs(df):
    df = df.withColumn(
        ESTIMATE_JOB_COUNT_2021,
        when(
            (col(ESTIMATE_JOB_COUNT_2021).isNull() & (col("job_count_2021").isNotNull())), col("job_count_2021")
        ).otherwise(col(ESTIMATE_JOB_COUNT_2021)),
    )

    return df


def model_non_res_historical(df):
    """
    Non-res : Historical :  : 2021 jobs = Last known value *1.03
    """
    # TODO: remove magic number 1.03
    df = df.withColumn(
        ESTIMATE_JOB_COUNT_2021,
        when(
            (
                col(ESTIMATE_JOB_COUNT_2021).isNull()
                & (col(PRIMARY_SERVICE_TYPE) == "non-residential")
                & col(LAST_KNOWN_JOB_COUNT).isNotNull()
            ),
            col(LAST_KNOWN_JOB_COUNT) * 1.03,
        ).otherwise(col(ESTIMATE_JOB_COUNT_2021)),
    )

    return df


def model_non_res_historical_pir(df):
    """
    Non-res : Not Historical : PIR : 2021 jobs = 25.046 + 0.469 * PIR service users
    """
    # TODO: remove magic number 25.046
    # TODO: remove magic number 0.469

    df = df.withColumn(
        ESTIMATE_JOB_COUNT_2021,
        when(
            (
                col(ESTIMATE_JOB_COUNT_2021).isNull()
                & (col(PRIMARY_SERVICE_TYPE) == "non-residential")
                & col(PIR_SERVICE_USERS).isNotNull()
            ),
            (25.046 + (0.469 * col(PIR_SERVICE_USERS))),
        ).otherwise(col(ESTIMATE_JOB_COUNT_2021)),
    )
    return df


def model_non_res_default(df):
    """
    Non-res : Not Historical : Not PIR : 2021 jobs = mean of known 2021 non-res jobs (54.09)
    """
    # TODO: remove magic number 54.09

    df = df.withColumn(
        ESTIMATE_JOB_COUNT_2021,
        when(
            (col(ESTIMATE_JOB_COUNT_2021).isNull() & (col(PRIMARY_SERVICE_TYPE) == "non-residential")), 54.09
        ).otherwise(col(ESTIMATE_JOB_COUNT_2021)),
    )

    return df


def model_care_home_with_nursing_historical(df):
    """
    Care home with nursing : Historical :  : 2021 jobs = Last known value * 1.004
    """
    # TODO: remove magic number 1.004

    df = df.withColumn(
        ESTIMATE_JOB_COUNT_2021,
        when(
            (
                col(ESTIMATE_JOB_COUNT_2021).isNull()
                & (col(PRIMARY_SERVICE_TYPE) == NURSING_HOME_IDENTIFIER)
                & col(LAST_KNOWN_JOB_COUNT).isNotNull()
            ),
            col(LAST_KNOWN_JOB_COUNT) * 1.004,
        ).otherwise(col(ESTIMATE_JOB_COUNT_2021)),
    )

    return df


def model_care_home_with_nursing_pir_and_cqc_beds(df):
    """
    Care home with nursing : Not Historical : PIR :  2021 jobs = (0.773*beds)+(0.551*PIR)+0.304
    """
    # TODO: remove magic number 0.773
    # TODO: remove magic number 0.551
    # TODO: remove magic number 0.304

    df = df.withColumn(
        ESTIMATE_JOB_COUNT_2021,
        when(
            (
                col(ESTIMATE_JOB_COUNT_2021).isNull()
                & (col(PRIMARY_SERVICE_TYPE) == "Care home with nursing")
                & col(PIR_SERVICE_USERS).isNotNull()
                & col(CQC_NUMBER_OF_BEDS).isNotNull()
            ),
            ((0.773 * col(CQC_NUMBER_OF_BEDS)) + (0.551 * col(PIR_SERVICE_USERS)) + 0.304),
        ).otherwise(col(ESTIMATE_JOB_COUNT_2021)),
    )

    return df


def model_care_home_with_nursing_cqc_beds(df):
    """
    Care home with nursing : Not Historical : Not PIR : 2021 jobs = (1.203*beds) +2.39
    """
    # TODO: remove magic number 1.203
    # TODO: remove magic number 2.39

    df = df.withColumn(
        ESTIMATE_JOB_COUNT_2021,
        when(
            (
                col(ESTIMATE_JOB_COUNT_2021).isNull()
                & (col(PRIMARY_SERVICE_TYPE) == "Care home with nursing")
                & col(CQC_NUMBER_OF_BEDS).isNotNull()
            ),
            (1.203 * col(CQC_NUMBER_OF_BEDS) + 2.39),
        ).otherwise(col(ESTIMATE_JOB_COUNT_2021)),
    )

    return df


def model_care_home_without_nursing_historical(df):
    """
    Care home without nursing : Historical :  : 2021 jobs = Last known value * 1.01
    """
    # TODO: remove magic number 1.01

    df = df.withColumn(
        ESTIMATE_JOB_COUNT_2021,
        when(
            (
                col(ESTIMATE_JOB_COUNT_2021).isNull()
                & (col(PRIMARY_SERVICE_TYPE) == "Care home without nursing")
                & col(LAST_KNOWN_JOB_COUNT).isNotNull()
            ),
            col(LAST_KNOWN_JOB_COUNT) * 1.01,
        ).otherwise(col(ESTIMATE_JOB_COUNT_2021)),
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
        ESTIMATE_JOB_COUNT_2021,
        when(
            (
                col(ESTIMATE_JOB_COUNT_2021).isNull()
                & (col(PRIMARY_SERVICE_TYPE) == "Care home without nursing")
                & col(PIR_SERVICE_USERS).isNotNull()
                & col(CQC_NUMBER_OF_BEDS).isNotNull()
            ),
            (10.652 + (0.571 * col(CQC_NUMBER_OF_BEDS)) + (0.296 * col(PIR_SERVICE_USERS))),
        ).otherwise(col(ESTIMATE_JOB_COUNT_2021)),
    )

    return df


def model_care_home_without_nursing_cqc_beds(df):
    """
    Care home without nursing : Not Historical : Not PIR : 2021 jobs = 11.291+(0.8126*beds)
    """
    # TODO: remove magic number 11.291
    # TODO: remove magic number 0.8126

    df = df.withColumn(
        ESTIMATE_JOB_COUNT_2021,
        when(
            (
                col(ESTIMATE_JOB_COUNT_2021).isNull()
                & (col(PRIMARY_SERVICE_TYPE) == "Care home without nursing")
                & col(CQC_NUMBER_OF_BEDS).isNotNull()
            ),
            (11.291 + (0.8126 * col(CQC_NUMBER_OF_BEDS))),
        ).otherwise(col(ESTIMATE_JOB_COUNT_2021)),
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
        "--destination",
        help="A destination directory for outputting cqc locations, if not provided shall default to S3 todays date.",
        required=True,
    )

    args, unknown = parser.parse_known_args()

    return args.prepared_locations_source, args.destination


if __name__ == "__main__":
    (
        prepared_locations_source,
        destination,
    ) = collect_arguments()

    main(prepared_locations_source, destination)
