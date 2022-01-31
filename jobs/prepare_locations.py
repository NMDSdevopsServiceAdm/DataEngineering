import argparse
from datetime import date

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import abs, coalesce, greatest, lit, max, when, col
from pyspark.sql.types import IntegerType
from utils import utils

required_workplace_fields = [
    "locationid",
    "providerid",
    "totalstaff",
    "wkrrecs",
    "year",
    "month",
    "day",
    "import_date",
    "version"
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
    "year",
    "month",
    "day",
    "import_date",
    "version"
]


def main(workplace_source, cqc_source, destination):
    spark = utils.get_spark()
    print(f"Reading workplaces parquet from {workplace_source}")
    workplaces_df = spark.read.parquet(
        workplace_source).select(required_workplace_fields)

    workplaces_df = remove_duplicates(workplaces_df)
    workplaces_df = clean(workplaces_df)
    workplaces_df = filter_nulls(workplaces_df)

    print(f"Reading CQC parquet from {workplace_source}")
    cqc_df = spark.read.parquet(
        cqc_source).select(required_cqc_fields)

    output_df = cqc_df.join(workplaces_df, "locationid", "left")

    output_df = calculate_jobcount(output_df)

    output_df.show(20, False)

    # print(f"Exporting as parquet to {destination}")
    # utils.write_to_parquet(workplaces_df, destination)

    print(f"Exporting as csv to {destination}")
    output_df.coalesce(1).write.format(
        "com.databricks.spark.csv").save(destination, header="true")


def remove_duplicates(input_df):
    print(f"Removing duplicates...")
    return input_df.drop_duplicates(subset=["locationid", "import_date"])


def clean(input_df):
    print(f"Cleaning...")
    # Standardise negative and 0 values as None.
    input_df = input_df.replace('0', None).replace('-1', None)

    # Cast integers to string
    input_df = input_df.withColumn(
        "totalstaff", input_df["totalstaff"].cast(IntegerType()))

    input_df = input_df.withColumn(
        "wkrrecs", input_df["wkrrecs"].cast(IntegerType()))

    return input_df


def filter_nulls(input_df):
    print(f"Filtering nulls...")
    # Remove rows with null for wkrrecs and totalstaff
    input_df = input_df.filter("wkrrecs is not null or totalstaff is not null")

    # Remove rows with null locationId
    input_df = input_df.na.drop(subset=["locationid"])

    return input_df


def calculate_jobcount(input_df):
    print(f"Calculating jobcount...")
    # Add null/empty jobcount column
    input_df = input_df.withColumn("jobcount", lit(None).cast(IntegerType()))

    # totalstaff = wkrrrecs: Take totalstaff
    input_df = input_df.withColumn("jobcount", when(
        (
            col("jobcount").isNull() &
            (col("wkrrecs") == col("totalstaff")) &
            col("totalstaff").isNotNull() &
            col("wkrrecs").isNotNull()
        ), col("totalstaff")
    ).otherwise(col("jobcount")))

    # Either wkrrecs or totalstaff is null: return first not null
    input_df = input_df.withColumn("jobcount", when(
        (
            col("jobcount").isNull() &
            (
                (
                    col("totalstaff").isNull() &
                    col("wkrrecs").isNotNull()
                ) |
                (
                    col("totalstaff").isNotNull() &
                    col("wkrrecs").isNull()
                )
            )
        ), coalesce(input_df.totalstaff, input_df.wkrrecs)
    ).otherwise(coalesce(col("jobcount"))))

    # Abs difference between totalstaff & wkrrecs < 5 or < 10% take average:
    input_df = input_df.withColumn('abs_difference', abs(
        input_df.totalstaff - input_df.wkrrecs))

    input_df = input_df.withColumn("jobcount", when(
        (
            col("jobcount").isNull() &
            (
                (col("abs_difference") < 5) | (
                    col("abs_difference") / col("totalstaff") < 0.1)
            )
        ), (col("totalstaff") + col("wkrrecs")) / 2
    ).otherwise(col("jobcount")))

    input_df = input_df.drop("abs_difference")

    # totalstaff or wkrrecs < 3: return max
    input_df = input_df.withColumn("jobcount", when(
        (
            col("jobcount").isNull() &
            (
                (col("totalstaff") < 3) | (col("wkrrecs") < 3)
            )
        ), greatest(col("totalstaff"), col("wkrrecs"))
    ).otherwise(col("jobcount")))

    # Estimate job count from beds
    input_df = input_df.withColumn("bed_estimate_jobcount", when(
        (
            col("jobcount").isNull() &
            (col("numberofbeds") > 0)
        ), (8.40975704621392 + (col("numberofbeds") * 1.0010753137758377001))
    ).otherwise(None))

    # Determine differences
    input_df = input_df.withColumn("totalstaff_diff", abs(
        input_df.totalstaff - input_df.bed_estimate_jobcount))
    input_df = input_df.withColumn("wkrrecs_diff", abs(
        input_df.wkrrecs - input_df.bed_estimate_jobcount))
    input_df = input_df.withColumn("totalstaff_percentage_diff", abs(
        input_df.totalstaff_diff/input_df.bed_estimate_jobcount))
    input_df = input_df.withColumn("wkrrecs_percentage_diff", abs(
        input_df.wkrrecs/input_df.bed_estimate_jobcount))

    # Bounding predictions to certain locations with differences in range
    # if totalstaff and wkrrecs within 10% or < 5: return avg(totalstaff + wkrrds)
    input_df = input_df.withColumn("jobcount", when(
        (
            col("jobcount").isNull() &
            col("bed_estimate_jobcount").isNotNull() &
            (
                (
                    (col("totalstaff_diff") < 5) | (
                        col("totalstaff_percentage_diff") < 0.1)
                ) &
                (
                    (col("wkrrecs_diff") < 5) | (
                        col("wkrrecs_percentage_diff") < 0.1)
                )
            )
        ), (col("totalstaff") + col("wkrrecs")) / 2
    ).otherwise(col("jobcount")))

    # if totalstaff within 10% or < 5: return totalstaff
    input_df = input_df.withColumn("jobcount", when(
        (
            col("jobcount").isNull() &
            col("bed_estimate_jobcount").isNotNull() &
            (

                (col("totalstaff_diff") < 5) | (
                    col("totalstaff_percentage_diff") < 0.1)

            )
        ), col("totalstaff")
    ).otherwise(col("jobcount")))

    # if wkrrecs within 10% or < 5: return wkrrecs
    input_df = input_df.withColumn("jobcount", when(
        (
            col("jobcount").isNull() &
            col("bed_estimate_jobcount").isNotNull() &
            (

                (col("wkrrecs_diff") < 5) | (
                    col("wkrrecs_percentage_diff") < 0.1)

            )
        ), col("wkrrecs")
    ).otherwise(col("jobcount")))

    # Drop temporary columns
    columns_to_drop = [
        "bed_estimate_jobcount",
        "totalstaff_diff",
        "wkrrecs_diff",
        "totalstaff_percentage_diff",
        "wkrrecs_percentage_diff"
    ]

    input_df = input_df.drop(*columns_to_drop)

    return input_df


def collect_arguments():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--workplace_source", help="Source s3 directory for ASCWDS workplace dataset", required=True)
    parser.add_argument(
        "--cqc_source", help="Source s3 directory for CQC locations api dataset", required=True)
    parser.add_argument(
        "--destination", help="A destination directory for outputting cqc locations, if not provided shall default to S3 todays date.", required=True)

    args, unknown = parser.parse_known_args()

    return args.workplace_source, args.cqc_source, args.destination


if __name__ == "__main__":
    workplace_source, cqc_source, destination, = collect_arguments()
    main(workplace_source, cqc_source, destination)
