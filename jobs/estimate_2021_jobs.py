import argparse

from pyspark.sql.functions import coalesce, col, lit, array_contains, when
from pyspark.sql.types import IntegerType

from environment import constants
from utils import utils


def main(prepared_locations_source, pir_source, cqc_locations_source, destination):
    spark = utils.get_spark()
    print("Estimating 2021 jobs")
    locations_df = (
        spark.read.parquet(prepared_locations_source)
        .select(col("locationid").distinct())
        .filter("registrationstatus = 'Registered' and type = 'Social Care Org'")
    )

    locations_df = locations_df.withColumn("estimate_jobcount_2021", lit(None).cast(IntegerType()))

    locations_df = collect_ascwds_historical_job_figures(spark, prepared_locations_source, locations_df)

    # Join PIR service users
    pir_df = (
        spark.read.option("basePath", constants.PIR_BASE_PATH)
        .parquet(pir_source)
        .select(
            col("location_id").alias("locationid"),
            col(
                "21_How_many_people_are_currently_receiving_support"
                "_with_regulated_activities_as_defined_by_the_Health"
                "_and_Social_Care_Act_from_your_service"
            ).alias("pir_service_users"),
        )
    )
    pir_df = pir_df.dropDuplicates(["locationid"])
    locations_df = locations_df.join(pir_df, "locationid", "left")

    # Join CQC for number of beds
    cqc_df = (
        spark.read.option("basePath", constants.CQC_LOCATIONS_BASE_PATH)
        .parquet(cqc_locations_source)
        .select(
            col("locationid"),
            col("gacservicetypes.description").alias("services"),
        )
    )

    cqc_df = cqc_df.dropDuplicates(["locationid"])
    locations_df = locations_df.join(cqc_df, "locationid", "left")

    locations_df = determine_ascwds_primary_service_type(locations_df)

    print("Completed estimated 2021 jobs")


def determine_ascwds_primary_service_type(input_df):
    return input_df.withColumn(
        "primary_service_type",
        when(array_contains(input_df.services, "Care home service with nursing"), "Care home with nursing")
        .when(array_contains(input_df.services, "Care home service without nursing"), "Care home without nursing")
        .otherwise("non-residential"),
    )


def collect_ascwds_historical_job_figures(spark, data_source, input_df):
    for year in ["2021", "2020", "2019"]:
        jobs_previous = spark.sql(
            f"""select
                locationid,
                max(jobcount) as jobcount_{year}
                from
                    {data_source}
                where
                    year(ascwds_workplace_import_date) = {year}
                group by year(ascwds_workplace_import_date), locationid
                having max(year(ascwds_workplace_import_date))
            """
        )
        input_df = input_df.join(jobs_previous, "locationid", "left")

    # Calculate last known jobs previous to 2021
    input_df = input_df.withColumn("last_known_job_count", coalesce("jobcount_2020", "jobcount_2019"))

    return input_df


def collect_arguments():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--prepared_locations_source",
        help="Source s3 directory for prepared_locations",
        required=True,
    )
    parser.add_argument(
        "--pir_source",
        help="Source s3 directory for pir dataset",
        required=True,
    )
    parser.add_argument(
        "--cqc_locations_source",
        help="Source s3 directory cqc locations dataset",
        required=True,
    )
    parser.add_argument(
        "--destination",
        help="A destination directory for outputting cqc locations, if not provided shall default to S3 todays date.",
        required=True,
    )

    args, unknown = parser.parse_known_args()

    return args.prepared_locations_source, args.pir_source, args.cqc_locations_source, args.destination


if __name__ == "__main__":
    (
        prepared_locations_source,
        pir_source,
        cqc_locations_source,
        destination,
    ) = collect_arguments()
    main(prepared_locations_source, pir_source, cqc_locations_source, destination)
