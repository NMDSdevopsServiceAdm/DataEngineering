import argparse
from datetime import date

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import abs, coalesce, greatest, lit, max, when, col, to_date, lower
from pyspark.sql.types import IntegerType
from utils import utils
from environment import constants

MIN_ABSOLUTE_DIFFERENCE = 5
MIN_PERCENTAGE_DIFFERENCE = 0.1


def main(workplace_source, cqc_location_source, cqc_provider_source, pir_source, destination):
    print("Building locations prepared dataset")

    ascwds_workplace_df = get_ascwds_workplace_df(workplace_source)

    cqc_location_df = get_cqc_location_df(cqc_location_source)
    output_df = cqc_location_df.join(ascwds_workplace_df, "locationid", "left")

    cqc_provider_df = get_cqc_provider_df(cqc_provider_source)
    cqc_provider_df = add_cqc_sector(cqc_provider_df)
    output_df = output_df.join(cqc_provider_df, "providerid", "left")
    output_df = filter_out_cqc_la_data(output_df)

    pir_df = get_pir_dataframe(pir_source)
    output_df = output_df.join(pir_df, "locationid", "left")

    output_df = calculate_jobcount(output_df)

    print(f"Exporting as parquet to {destination}")
    utils.write_to_parquet(output_df, destination)


def get_ascwds_workplace_df(workplace_source, base_path=constants.ASCWDS_WORKPLACE_BASE_PATH):
    spark = utils.get_spark()
    print(f"Reading workplaces parquet from {workplace_source}")
    workplace_df = (
        spark.read.option("basePath", base_path)
        .parquet(workplace_source)
        .select(
            col("locationid"),
            col("establishmentid"),
            col("providerid"),
            col("totalstaff").alias("total_staff"),
            col("wkrrecs").alias("worker_record_count"),
            col("import_date").alias("ascwds_workplace_import_date"),
        )
    )

    # Format date
    workplace_df = workplace_df.withColumn(
        "ascwds_workplace_import_date", to_date(col("ascwds_workplace_import_date").cast("string"), "yyyyMMdd")
    )

    workplace_df = workplace_df.drop_duplicates(subset=["locationid", "ascwds_workplace_import_date"])
    workplace_df = clean(workplace_df)
    workplace_df = filter_nulls(workplace_df)

    return workplace_df


def get_cqc_location_df(cqc_location_source, base_path=constants.CQC_LOCATIONS_BASE_PATH):
    spark = utils.get_spark()

    print(f"Reading CQC locations parquet from {cqc_location_source}")
    cqc_df = (
        spark.read.option("basePath", base_path)
        .parquet(cqc_location_source)
        .select(
            col("locationid"),
            col("organisationtype").alias("organisation_type"),
            col("type").alias("location_type"),
            col("name").alias("location_name"),
            col("registrationstatus").alias("registration_status"),
            col("registrationdate").alias("registration_date"),
            col("deregistrationdate").alias("deregistration_date"),
            col("dormancy"),
            col("numberofbeds").alias("number_of_beds"),
            col("region"),
            col("postalcode").alias("postal_code"),
            col("carehome"),
            col("constituency"),
            col("localauthority").alias("local_authority"),
            col("gacservicetypes.description").alias("services_offered"),
            col("import_date").alias("cqc_locations_import_date"),
        )
    )

    # Format date
    cqc_df = cqc_df.withColumn(
        "cqc_locations_import_date", to_date(col("cqc_locations_import_date").cast("string"), "yyyyMMdd")
    )

    cqc_df = cqc_df.filter("location_type=='Social Care Org'")

    return cqc_df


def get_cqc_provider_df(cqc_provider_source, base_path=constants.CQC_PROVIDERS_BASE_PATH):
    spark = utils.get_spark()

    print(f"Reading CQC providers parquet from {cqc_provider_source}")
    cqc_provider_df = (
        spark.read.option("basePath", base_path)
        .parquet(cqc_provider_source)
        .select(
            col("providerid"), col("name").alias("provider_name"), col("import_date").alias("cqc_providers_import_date")
        )
    )

    # Format date
    cqc_provider_df = cqc_provider_df.withColumn(
        "cqc_providers_import_date", to_date(col("cqc_providers_import_date").cast("string"), "yyyyMMdd")
    )

    return cqc_provider_df


def get_pir_dataframe(pir_source, base_path=constants.PIR_BASE_PATH):
    spark = utils.get_spark()

    # Join PIR service users
    print(f"Reading PIR parquet from {pir_source}")
    pir_df = (
        spark.read.option("basePath", base_path)
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

    return pir_df


def clean(input_df):
    print("Cleaning...")

    # Standardise negative and 0 values as None.
    input_df = input_df.replace("0", None).replace("-1", None)

    # Cast strings to integers
    input_df = input_df.withColumn("total_staff", input_df["total_staff"].cast(IntegerType()))

    input_df = input_df.withColumn("worker_record_count", input_df["worker_record_count"].cast(IntegerType()))

    return input_df


def filter_nulls(input_df):
    print("Filtering nulls...")
    # Remove rows with null for worker_record_count and total_staff
    input_df = input_df.filter("worker_record_count is not null or total_staff is not null")

    # Remove rows with null locationId
    input_df = input_df.na.drop(subset=["locationid"])

    return input_df


def add_cqc_sector(input_df):
    print("Adding CQC sector column...")

    # allocate service based on provider name
    input_df = input_df.withColumn(
        "cqc_sector",
        lower(input_df.provider_name).rlike(
            "department of community services|social & community services|scc adult social care|cheshire west and chester reablement service|council|social services|mbc|mdc|london borough|royal borough|borough of"
        )
        & ~lower(input_df.provider_name).rlike("the council of st monica trust"),
    )

    input_df = input_df.withColumn(
        "cqc_sector", when(input_df.cqc_sector == "false", "Independent").otherwise("Local authority")
    )

    return input_df


def filter_out_cqc_la_data(input_df):
    print("Filter out LA sector data...")

    # remove any records where sector is 'local authority'
    input_df = input_df.filter("cqc_sector=='Independent'")

    return input_df


def calculate_jobcount_totalstaff_equal_wkrrecs(input_df):
    # total_staff = wkrrrecs: Take total_staff
    return input_df.withColumn(
        "job_count",
        when(
            (
                col("job_count").isNull()
                & (col("worker_record_count") == col("total_staff"))
                & col("total_staff").isNotNull()
                & col("worker_record_count").isNotNull()
            ),
            col("total_staff"),
        ).otherwise(col("job_count")),
    )


def calculate_jobcount_coalesce_totalstaff_wkrrecs(input_df):
    # Either worker_record_count or total_staff is null: return first not null
    return input_df.withColumn(
        "job_count",
        when(
            (
                col("job_count").isNull()
                & (
                    (col("total_staff").isNull() & col("worker_record_count").isNotNull())
                    | (col("total_staff").isNotNull() & col("worker_record_count").isNull())
                )
            ),
            coalesce(input_df.total_staff, input_df.worker_record_count),
        ).otherwise(coalesce(col("job_count"))),
    )


def calculate_jobcount_abs_difference_within_range(input_df):
    # Abs difference between total_staff & worker_record_count < 5 or < 10% take average:
    input_df = input_df.withColumn("abs_difference", abs(input_df.total_staff - input_df.worker_record_count))

    input_df = input_df.withColumn(
        "job_count",
        when(
            (
                col("job_count").isNull()
                & (
                    (col("abs_difference") < MIN_ABSOLUTE_DIFFERENCE)
                    | (col("abs_difference") / col("total_staff") < MIN_PERCENTAGE_DIFFERENCE)
                )
            ),
            (col("total_staff") + col("worker_record_count")) / 2,
        ).otherwise(col("job_count")),
    )

    input_df = input_df.drop("abs_difference")

    return input_df


def calculate_jobcount_handle_tiny_values(input_df):
    # total_staff or worker_record_count < 3: return max
    return input_df.withColumn(
        "job_count",
        when(
            (col("job_count").isNull() & ((col("total_staff") < 3) | (col("worker_record_count") < 3))),
            greatest(col("total_staff"), col("worker_record_count")),
        ).otherwise(col("job_count")),
    )


def calculate_jobcount_estimate_from_beds(input_df):
    beds_to_jobcount_intercept = 8.40975704621392
    beds_to_jobcount_coefficient = 1.0010753137758377001
    input_df = input_df.withColumn(
        "bed_estimate_jobcount",
        when(
            (col("job_count").isNull() & (col("number_of_beds") > 0)),
            (beds_to_jobcount_intercept + (col("number_of_beds") * beds_to_jobcount_coefficient)),
        ).otherwise(None),
    )

    # Determine differences
    input_df = input_df.withColumn("totalstaff_diff", abs(input_df.total_staff - input_df.bed_estimate_jobcount))
    input_df = input_df.withColumn("wkrrecs_diff", abs(input_df.worker_record_count - input_df.bed_estimate_jobcount))
    input_df = input_df.withColumn(
        "totalstaff_percentage_diff",
        abs(input_df.totalstaff_diff / input_df.bed_estimate_jobcount),
    )
    input_df = input_df.withColumn(
        "wkrrecs_percentage_diff",
        abs(input_df.worker_record_count / input_df.bed_estimate_jobcount),
    )

    # Bounding predictions to certain locations with differences in range
    # if total_staff and worker_record_count within 10% or < 5: return avg(total_staff + wkrrds)
    input_df = input_df.withColumn(
        "job_count",
        when(
            (
                col("job_count").isNull()
                & col("bed_estimate_jobcount").isNotNull()
                & (
                    (
                        (col("totalstaff_diff") < MIN_ABSOLUTE_DIFFERENCE)
                        | (col("totalstaff_percentage_diff") < MIN_PERCENTAGE_DIFFERENCE)
                    )
                    & (
                        (col("wkrrecs_diff") < MIN_ABSOLUTE_DIFFERENCE)
                        | (col("wkrrecs_percentage_diff") < MIN_PERCENTAGE_DIFFERENCE)
                    )
                )
            ),
            (col("total_staff") + col("worker_record_count")) / 2,
        ).otherwise(col("job_count")),
    )

    # if total_staff within 10% or < 5: return total_staff
    input_df = input_df.withColumn(
        "job_count",
        when(
            (
                col("job_count").isNull()
                & col("bed_estimate_jobcount").isNotNull()
                & (
                    (col("totalstaff_diff") < MIN_ABSOLUTE_DIFFERENCE)
                    | (col("totalstaff_percentage_diff") < MIN_PERCENTAGE_DIFFERENCE)
                )
            ),
            col("total_staff"),
        ).otherwise(col("job_count")),
    )

    # if worker_record_count within 10% or < 5: return worker_record_count
    input_df = input_df.withColumn(
        "job_count",
        when(
            (
                col("job_count").isNull()
                & col("bed_estimate_jobcount").isNotNull()
                & (
                    (col("wkrrecs_diff") < MIN_ABSOLUTE_DIFFERENCE)
                    | (col("wkrrecs_percentage_diff") < MIN_PERCENTAGE_DIFFERENCE)
                )
            ),
            col("worker_record_count"),
        ).otherwise(col("job_count")),
    )

    # Drop temporary columns
    columns_to_drop = [
        "bed_estimate_jobcount",
        "totalstaff_diff",
        "wkrrecs_diff",
        "totalstaff_percentage_diff",
        "wkrrecs_percentage_diff",
    ]

    input_df = input_df.drop(*columns_to_drop)

    return input_df


def calculate_jobcount(input_df):
    print("Calculating job_count...")

    # Add null/empty job_count column
    input_df = input_df.withColumn("job_count", lit(None).cast(IntegerType()))

    input_df = calculate_jobcount_totalstaff_equal_wkrrecs(input_df)
    input_df = calculate_jobcount_coalesce_totalstaff_wkrrecs(input_df)
    input_df = calculate_jobcount_abs_difference_within_range(input_df)
    input_df = calculate_jobcount_handle_tiny_values(input_df)
    input_df = calculate_jobcount_estimate_from_beds(input_df)

    return input_df


def collect_arguments():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--workplace_source",
        help="Source s3 directory for ASCWDS workplace dataset",
        required=True,
    )
    parser.add_argument(
        "--cqc_location_source",
        help="Source s3 directory for CQC locations api dataset",
        required=True,
    )
    parser.add_argument(
        "--cqc_provider_source",
        help="Source s3 directory for CQC providers api dataset",
        required=True,
    )
    parser.add_argument(
        "--pir_source",
        help="Source s3 directory for pir dataset",
        required=True,
    )
    parser.add_argument(
        "--destination",
        help="A destination directory for outputting cqc locations, if not provided shall default to S3 todays date.",
        required=True,
    )

    args, unknown = parser.parse_known_args()

    return (
        args.workplace_source,
        args.cqc_location_source,
        args.cqc_provider_source,
        args.pir_source,
        args.destination,
    )


if __name__ == "__main__":
    (
        workplace_source,
        cqc_location_source,
        cqc_provider_source,
        pir_source,
        destination,
    ) = collect_arguments()
    main(workplace_source, cqc_location_source, cqc_provider_source, pir_source, destination)
