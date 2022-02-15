import argparse
from datetime import date

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import abs, coalesce, greatest, lit, max, when, col, to_date
from pyspark.sql.types import IntegerType
from utils import utils
from environment import constants

required_workplace_fields = [
    "locationid",
    "establishmentid",
    "providerid",
    "totalstaff",
    "wkrrecs",
    "import_date",
]

required_cqc_fields = [
    "locationid",
    "organisationtype",
    "type",
    "name",
    "registrationstatus",
    "registrationdate",
    "deregistrationdate",
    "dormancy",
    "numberofbeds",
    "region",
    "postalcode",
    "carehome",
    "constituency",
    "localauthority",
    "import_date",
]

MIN_ABSOLUTE_DIFFERENCE = 5
MIN_PERCENTAGE_DIFFERENCE = 0.1


def main(workplace_source, cqc_location_source, cqc_provider_source, destination):
    spark = utils.get_spark()
    print(f"Reading workplaces parquet from {workplace_source}")
    workplaces_df = (
        spark.read.option("basePath", constants.ASCWDS_WORKPLACE_BASE_PATH)
        .parquet(workplace_source)
        .select(required_workplace_fields)
    )

    workplaces_df = format_date(workplaces_df, "import_date", "ascwds_workplace_import_date")
    workplaces_df = remove_duplicates(workplaces_df)
    workplaces_df = clean(workplaces_df)
    workplaces_df = filter_nulls(workplaces_df)

    print(f"Reading CQC locations parquet from {cqc_location_source}")
    cqc_df = (
        spark.read.option("basePath", constants.CQC_LOCATIONS_BASE_PATH)
        .parquet(cqc_location_source)
        .select(required_cqc_fields)
    )

    cqc_df = format_date(cqc_df, "import_date", "cqc_locations_import_date")

    print(f"Reading CQC providers parquet from {cqc_provider_source}")
    cqc_provider_df = (
        spark.read.option("basePath", constants.CQC_PROVIDERS_BASE_PATH)
        .parquet(cqc_provider_source)
        .select("providerid", col("name").alias("provider_name"), col("import_date"))
    )

    cqc_provider_df = format_date(cqc_provider_df, "import_date", "cqc_providers_import_date")

    output_df = cqc_df.join(workplaces_df, "locationid", "left")
    output_df = output_df.join(cqc_provider_df, "providerid", "left")

    output_df = calculate_jobcount(output_df)

    print(f"Exporting as parquet to {destination}")
    utils.write_to_parquet(output_df, destination)


def format_date(input_df, date_column, new_date_column):
    input_df = input_df.withColumnRenamed(date_column, new_date_column)

    input_df = input_df.withColumn(new_date_column, to_date(col(new_date_column).cast("string"), "yyyyMMdd"))

    return input_df


def remove_duplicates(input_df):
    print("Removing duplicates...")
    return input_df.drop_duplicates(subset=["locationid", "ascwds_workplace_import_date"])


def clean(input_df):
    print("Cleaning...")

    # Standardise negative and 0 values as None.
    input_df = input_df.replace("0", None).replace("-1", None)

    # Cast integers to string
    input_df = input_df.withColumn("totalstaff", input_df["totalstaff"].cast(IntegerType()))

    input_df = input_df.withColumn("wkrrecs", input_df["wkrrecs"].cast(IntegerType()))

    return input_df


def filter_nulls(input_df):
    print("Filtering nulls...")
    # Remove rows with null for wkrrecs and totalstaff
    input_df = input_df.filter("wkrrecs is not null or totalstaff is not null")

    # Remove rows with null locationId
    input_df = input_df.na.drop(subset=["locationid"])

    return input_df


def calculate_jobcount_totalstaff_equal_wkrrecs(input_df):
    # totalstaff = wkrrrecs: Take totalstaff
    return input_df.withColumn(
        "jobcount",
        when(
            (
                col("jobcount").isNull()
                & (col("wkrrecs") == col("totalstaff"))
                & col("totalstaff").isNotNull()
                & col("wkrrecs").isNotNull()
            ),
            col("totalstaff"),
        ).otherwise(col("jobcount")),
    )


def calculate_jobcount_coalesce_totalstaff_wkrrecs(input_df):
    # Either wkrrecs or totalstaff is null: return first not null
    return input_df.withColumn(
        "jobcount",
        when(
            (
                col("jobcount").isNull()
                & (
                    (col("totalstaff").isNull() & col("wkrrecs").isNotNull())
                    | (col("totalstaff").isNotNull() & col("wkrrecs").isNull())
                )
            ),
            coalesce(input_df.totalstaff, input_df.wkrrecs),
        ).otherwise(coalesce(col("jobcount"))),
    )


def calculate_jobcount_abs_difference_within_range(input_df):
    # Abs difference between totalstaff & wkrrecs < 5 or < 10% take average:
    input_df = input_df.withColumn("abs_difference", abs(input_df.totalstaff - input_df.wkrrecs))

    input_df = input_df.withColumn(
        "jobcount",
        when(
            (
                col("jobcount").isNull()
                & (
                    (col("abs_difference") < MIN_ABSOLUTE_DIFFERENCE)
                    | (col("abs_difference") / col("totalstaff") < MIN_PERCENTAGE_DIFFERENCE)
                )
            ),
            (col("totalstaff") + col("wkrrecs")) / 2,
        ).otherwise(col("jobcount")),
    )

    input_df = input_df.drop("abs_difference")

    return input_df


def calculate_jobcount_handle_tiny_values(input_df):
    # totalstaff or wkrrecs < 3: return max
    return input_df.withColumn(
        "jobcount",
        when(
            (col("jobcount").isNull() & ((col("totalstaff") < 3) | (col("wkrrecs") < 3))),
            greatest(col("totalstaff"), col("wkrrecs")),
        ).otherwise(col("jobcount")),
    )


def calculate_jobcount_estimate_from_beds(input_df):
    beds_to_jobcount_intercept = 8.40975704621392
    beds_to_jobcount_coefficient = 1.0010753137758377001
    input_df = input_df.withColumn(
        "bed_estimate_jobcount",
        when(
            (col("jobcount").isNull() & (col("numberofbeds") > 0)),
            (beds_to_jobcount_intercept + (col("numberofbeds") * beds_to_jobcount_coefficient)),
        ).otherwise(None),
    )

    # Determine differences
    input_df = input_df.withColumn("totalstaff_diff", abs(input_df.totalstaff - input_df.bed_estimate_jobcount))
    input_df = input_df.withColumn("wkrrecs_diff", abs(input_df.wkrrecs - input_df.bed_estimate_jobcount))
    input_df = input_df.withColumn(
        "totalstaff_percentage_diff",
        abs(input_df.totalstaff_diff / input_df.bed_estimate_jobcount),
    )
    input_df = input_df.withColumn(
        "wkrrecs_percentage_diff",
        abs(input_df.wkrrecs / input_df.bed_estimate_jobcount),
    )

    # Bounding predictions to certain locations with differences in range
    # if totalstaff and wkrrecs within 10% or < 5: return avg(totalstaff + wkrrds)
    input_df = input_df.withColumn(
        "jobcount",
        when(
            (
                col("jobcount").isNull()
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
            (col("totalstaff") + col("wkrrecs")) / 2,
        ).otherwise(col("jobcount")),
    )

    # if totalstaff within 10% or < 5: return totalstaff
    input_df = input_df.withColumn(
        "jobcount",
        when(
            (
                col("jobcount").isNull()
                & col("bed_estimate_jobcount").isNotNull()
                & (
                    (col("totalstaff_diff") < MIN_ABSOLUTE_DIFFERENCE)
                    | (col("totalstaff_percentage_diff") < MIN_PERCENTAGE_DIFFERENCE)
                )
            ),
            col("totalstaff"),
        ).otherwise(col("jobcount")),
    )

    # if wkrrecs within 10% or < 5: return wkrrecs
    input_df = input_df.withColumn(
        "jobcount",
        when(
            (
                col("jobcount").isNull()
                & col("bed_estimate_jobcount").isNotNull()
                & (
                    (col("wkrrecs_diff") < MIN_ABSOLUTE_DIFFERENCE)
                    | (col("wkrrecs_percentage_diff") < MIN_PERCENTAGE_DIFFERENCE)
                )
            ),
            col("wkrrecs"),
        ).otherwise(col("jobcount")),
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
    print("Calculating jobcount...")

    # Add null/empty jobcount column
    input_df = input_df.withColumn("jobcount", lit(None).cast(IntegerType()))

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
        "--destination",
        help="A destination directory for outputting cqc locations, if not provided shall default to S3 todays date.",
        required=True,
    )

    args, unknown = parser.parse_known_args()

    return (
        args.workplace_source,
        args.cqc_location_source,
        args.cqc_provider_source,
        args.destination,
    )


if __name__ == "__main__":
    (
        workplace_source,
        cqc_location_source,
        cqc_provider_source,
        destination,
    ) = collect_arguments()
    main(workplace_source, cqc_location_source, cqc_provider_source, destination)
