import os
import argparse
from datetime import datetime
import builtins
from select import select
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import abs, coalesce, greatest, lit, max, when, col, to_date, add_months, lower, min
from pyspark.sql.types import IntegerType
from utils import utils
from environment import environment
from pyspark.sql.types import DateType, StructType, StructField


MIN_ABSOLUTE_DIFFERENCE = 5
MIN_PERCENTAGE_DIFFERENCE = 0.1


def main(workplace_source, cqc_location_source, cqc_provider_source, pir_source, destination=None):

    print("Building locations prepared dataset")
    master_df = None

    complete_ascwds_workplace_df = get_ascwds_workplace_df(workplace_source)
    complete_cqc_location_df = get_cqc_location_df(cqc_location_source)
    complete_cqc_provider_df = get_cqc_provider_df(cqc_provider_source)
    complete_pir_df = get_pir_df(pir_source)

    date_matrix = generate_closest_date_matrix(
        complete_ascwds_workplace_df, complete_cqc_location_df, complete_cqc_provider_df, complete_pir_df
    )

    date_matrix = date_matrix.collect()

    for snapshot_date_row in date_matrix:
        ascwds_workplace_df = complete_ascwds_workplace_df.filter(
            col("import_date") == snapshot_date_row["asc_workplace_date"]
        )

        ascwds_workplace_df = purge_workplaces(ascwds_workplace_df)

        cqc_locations_df = complete_cqc_location_df.filter(col("import_date") == snapshot_date_row["cqc_location_date"])
        cqc_providers_df = complete_cqc_provider_df.filter(col("import_date") == snapshot_date_row["cqc_provider_date"])
        pir_df = complete_pir_df.filter(col("import_date") == snapshot_date_row["pir_date"])

        # Rename import_date columns to ensure uniqueness
        ascwds_workplace_df = ascwds_workplace_df.withColumnRenamed("import_date", "ascwds_workplace_import_date")
        cqc_locations_df = cqc_locations_df.withColumnRenamed("import_date", "cqc_locations_import_date")
        cqc_providers_df = cqc_providers_df.withColumnRenamed("import_date", "cqc_providers_import_date")
        pir_df = pir_df.withColumnRenamed("import_date", "cqc_pir_import_date")

        output_df = cqc_locations_df.join(cqc_providers_df, "providerid", "left")
        output_df = output_df.join(ascwds_workplace_df, "locationid", "left")
        output_df = output_df.join(pir_df, "locationid", "left")
        output_df = calculate_jobcount(output_df)

        output_df = output_df.withColumn("snapshot_date", lit(snapshot_date_row["snapshot_date"]))

        if master_df is None:
            master_df = output_df
        else:
            master_df = master_df.union(output_df)

    master_df = master_df.select(
        "snapshot_date",
        "ascwds_workplace_import_date",
        "cqc_locations_import_date",
        "cqc_providers_import_date",
        "cqc_pir_import_date",
        "locationid",
        "location_type",
        "location_name",
        "organisation_type",
        "providerid",
        "provider_name",
        "orgid",
        "establishmentid",
        "registration_status",
        "registration_date",
        "deregistration_date",
        "carehome",
        "dormancy",
        "number_of_beds",
        "services_offered",
        "pir_service_users",
        "job_count",
        "region",
        "postal_code",
        "constituency",
        "local_authority",
        "cqc_sector",
    )

    if destination:
        print(f"Exporting as parquet to {destination}")
        utils.write_to_parquet(master_df, destination)
    else:
        return master_df


def get_ascwds_workplace_df(workplace_source, import_date=None, base_path=None):
    spark = utils.get_spark()

    if base_path is None:
        base_path = environment.get_ascwds_base_path()

    print(f"Reading workplaces parquet from {workplace_source}")
    workplace_df = (
        spark.read.option("basePath", base_path)
        .parquet(workplace_source)
        .select(
            col("locationid"),
            col("establishmentid"),
            col("totalstaff").alias("total_staff"),
            col("wkrrecs").alias("worker_record_count"),
            col("import_date"),
            col("orgid"),
            col("mupddate"),
            col("isparent"),
        )
    )

    # Format date
    workplace_df = format_import_date(workplace_df)

    workplace_df = workplace_df.drop_duplicates(subset=["locationid", "import_date"])
    workplace_df = clean(workplace_df)

    if import_date is not None:
        workplace_df = workplace_df.filter(col("import_date") == import_date)

    return workplace_df


def get_cqc_location_df(cqc_location_source, import_date=None, base_path=None):
    spark = utils.get_spark()

    if base_path is None:
        base_path = environment.get_cqc_locations_base_path()

    print(f"Reading CQC locations parquet from {cqc_location_source}")
    cqc_df = (
        spark.read.option("basePath", base_path)
        .parquet(cqc_location_source)
        .select(
            col("locationid"),
            col("providerid"),
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
            col("import_date"),
        )
    )

    cqc_df = format_import_date(cqc_df)

    cqc_df = cqc_df.filter("location_type=='Social Care Org'")

    if import_date is not None:
        cqc_df = cqc_df.filter(col("import_date") == import_date)

    return cqc_df


def get_cqc_provider_df(cqc_provider_source, import_date=None, base_path=None):
    spark = utils.get_spark()

    if base_path is None:
        base_path = environment.get_cqc_providers_base_path()

    print(f"Reading CQC providers parquet from {cqc_provider_source}")
    cqc_provider_df = (
        spark.read.option("basePath", base_path)
        .parquet(cqc_provider_source)
        .select(col("providerid"), col("name").alias("provider_name"), col("import_date"))
    )

    cqc_provider_df = add_cqc_sector(cqc_provider_df)

    cqc_provider_df = format_import_date(cqc_provider_df)

    if import_date is not None:
        cqc_provider_df = cqc_provider_df.filter(col("import_date") == import_date)

    return cqc_provider_df


def get_pir_df(pir_source, import_date=None, base_path=None):
    spark = utils.get_spark()

    if base_path is None:
        base_path = environment.get_pir_base_path()

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
            col("import_date"),
        )
    )

    pir_df = format_import_date(pir_df)

    pir_df = pir_df.dropDuplicates(["locationid", "import_date"])

    if import_date is not None:
        pir_df = pir_df.filter(col("import_date") == import_date)

    return pir_df


def get_unique_import_dates(df):
    distinct_ordered_import_date_df = df.select("import_date").distinct().orderBy("import_date")
    distinct_ordered_import_date_list = distinct_ordered_import_date_df.rdd.flatMap(lambda x: x).collect()
    return distinct_ordered_import_date_list


def get_date_closest_to_search_date(search_date, date_list):
    if search_date in date_list:
        return search_date

    try:
        closest_date = builtins.max(d for d in date_list if d < search_date)
    except ValueError as e:
        # No dates in search_list provided less than search date
        return None

    return closest_date


def format_import_date(df, fieldname="import_date"):
    return df.withColumn(fieldname, to_date(col(fieldname).cast("string"), "yyyyMMdd"))


def generate_closest_date_matrix(dataset_workplace, dataset_locations_api, dataset_providers_api, dataset_pir):
    spark = utils.get_spark()

    unique_asc_dates = get_unique_import_dates(dataset_workplace)
    unique_cqc_location_dates = get_unique_import_dates(dataset_locations_api)
    unique_cqc_provider_dates = get_unique_import_dates(dataset_providers_api)
    unique_pir_dates = get_unique_import_dates(dataset_pir)

    closest_cqc_location_dates = []
    for date in unique_asc_dates:
        closest_cqc_location_dates.append(get_date_closest_to_search_date(date, unique_cqc_location_dates))

    closest_cqc_provider_dates = []
    for date in unique_asc_dates:
        closest_cqc_provider_dates.append(get_date_closest_to_search_date(date, unique_cqc_provider_dates))

    closest_pir_dates = []
    for date in unique_asc_dates:
        closest_pir_dates.append(get_date_closest_to_search_date(date, unique_pir_dates))

    transpose = []
    for i in range(len(unique_asc_dates)):
        transpose.append(
            (
                unique_asc_dates[i],
                unique_asc_dates[i],
                closest_cqc_location_dates[i],
                closest_cqc_provider_dates[i],
                closest_pir_dates[i],
            )
        )

    schema = StructType(
        [
            StructField("snapshot_date", DateType(), True),
            StructField("asc_workplace_date", DateType(), True),
            StructField("cqc_location_date", DateType(), True),
            StructField("cqc_provider_date", DateType(), True),
            StructField("pir_date", DateType(), True),
        ]
    )
    date_matrix_df = spark.createDataFrame(data=transpose, schema=schema)

    return date_matrix_df


def clean(input_df):
    print("Cleaning...")

    # Standardise negative and 0 values as None.
    input_df = input_df.replace("0", None).replace("-1", None)

    # Cast strings to integers
    input_df = input_df.withColumn("total_staff", input_df["total_staff"].cast(IntegerType()))

    input_df = input_df.withColumn("worker_record_count", input_df["worker_record_count"].cast(IntegerType()))

    return input_df


def purge_workplaces(input_df):
    # Remove all locations that haven't been update for two years
    print("Purging ASCWDS accounts...")

    # Convert import_date to date field and remove 2 years
    input_df = input_df.withColumn("purge_date", add_months(col("import_date"), -24))

    # if the org is a parent, use the max mupddate for all locations at the org
    org_purge_df = (
        input_df.select("locationid", "orgid", "mupddate", "import_date")
        .groupBy("orgid", "import_date")
        .agg(max("mupddate").alias("mupddate_org"))
    )
    input_df = input_df.join(org_purge_df, ["orgid", "import_date"], "left")
    input_df = input_df.withColumn(
        "date_for_purge", when((input_df.isparent == "1"), input_df.mupddate_org).otherwise(input_df.mupddate)
    )

    # Remove ASCWDS accounts which haven't been updated in the 2 years prior to importing
    input_df = input_df.filter(input_df.purge_date < input_df.date_for_purge)

    input_df.drop("isparent", "mupddate")

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
        "--environment",
        help="Environment the job is running. Currently supports 'prod' and 'dev'",
        required=True,
    )
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
        args.environment,
        args.workplace_source,
        args.cqc_location_source,
        args.cqc_provider_source,
        args.pir_source,
        args.destination,
    )


if __name__ == "__main__":
    (
        env,
        workplace_source,
        cqc_location_source,
        cqc_provider_source,
        pir_source,
        destination,
    ) = collect_arguments()

    os.environ[environment.OS_ENVIRONEMNT_VARIABLE] = env
    main(workplace_source, cqc_location_source, cqc_provider_source, pir_source, destination)
