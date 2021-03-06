import argparse
import sys
import json
import re
from functools import partial

import pyspark.sql.functions as F
from pyspark.sql.types import StringType, FloatType, IntegerType

from schemas.worker_schema import WORKER_SCHEMA
from utils import utils


def main(source, schema, destination=None):
    last_processed_date = utils.get_max_snapshot_partitions(destination)
    if last_processed_date is not None:
        last_processed_date = (
            f"{last_processed_date[0]}{last_processed_date[1]}{last_processed_date[2]}"
        )
    main_df = get_dataset_worker(source, schema, last_processed_date)

    # TODO: Use snapshot year/month/day from prepared locations when joining with it
    main_df = main_df.withColumn(
        "snapshot_year", F.substring(main_df.import_date, 0, 4)
    )
    main_df = main_df.withColumn(
        "snapshot_month", F.substring(main_df.import_date, 5, 2)
    )
    main_df = main_df.withColumn("snapshot_day", F.substring(main_df.import_date, 7, 2))

    print("Formating date fields")
    main_df = utils.format_import_date(main_df)
    main_df = utils.format_date_fields(main_df)

    columns_to_be_aggregated_patterns = {
        "training": {
            "cols_to_aggregate": utils.extract_col_with_pattern(
                r"^tr\d\d[a-z]+", schema
            ),
            "udf_function": get_training_into_json,
            "types": utils.extract_specific_column_types(r"^tr\d\dflag$", schema),
        },
        "job_role": {
            "cols_to_aggregate": utils.extract_col_with_pattern(
                r"^jr\d\d[a-z]+", schema
            ),
            "udf_function": get_job_role_into_json,
            "types": utils.extract_col_with_pattern(r"^jr\d\d[a-z]", schema),
        },
        "qualifications": {
            "cols_to_aggregate": utils.extract_col_with_pattern(
                r"^ql\d{1,3}[a-z]+.", schema
            ),
            "udf_function": get_qualification_into_json,
            "types": utils.extract_col_with_pattern(
                r"^ql\d{1,3}(achq|app)(\d*|e)", schema
            ),
        },
    }
    for col_name, info in columns_to_be_aggregated_patterns.items():
        print(f"Aggregating {col_name}")
        main_df = replace_columns_with_aggregated_column(
            main_df,
            col_name,
            udf_function=info["udf_function"],
            cols_to_aggregate=info["cols_to_aggregate"],
            types=info["types"],
        )

    print("Aggregating hours worked")
    main_df = replace_columns_with_aggregated_column(
        main_df,
        "hrs_worked",
        udf_function=calculate_hours_worked,
        cols_to_aggregate=["emplstat", "zerohours", "averagehours", "conthrs"],
        cols_to_remove=["averagehours", "conthrs"],
        output_type=FloatType(),
    )

    print("Aggregating hourly rate")
    main_df = replace_columns_with_aggregated_column(
        main_df,
        "hourly_rate",
        udf_function=calculate_hourly_pay,
        cols_to_aggregate=["salary", "salaryint", "hrlyrate", "hrs_worked"],
        cols_to_remove=["salary", "salaryint", "hrlyrate"],
        output_type=FloatType(),
    )

    if destination:
        print(f"Exporting as parquet to {destination}")
        utils.write_to_parquet(
            main_df,
            destination,
            append=True,
            partitionKeys=["snapshot_year", "snapshot_month", "snapshot_day"],
        )
    else:
        return main_df


def get_dataset_worker(source, schema, since_date=None):
    spark = utils.get_spark()
    column_names = utils.extract_column_from_schema(schema)

    print(f"Reading worker parquet from {source}")
    worker_df_v0 = spark.read.option("basePath", source).parquet(
        f"{source}version=0.0.1/"
    )
    worker_df_v1 = spark.read.option("basePath", source).parquet(
        f"{source}version=1.0.0/"
    )

    for column in column_names:
        if column not in worker_df_v0.columns:
            worker_df_v0 = worker_df_v0.withColumn(column, F.lit(None))

    worker_df_v0 = worker_df_v0.select(column_names)
    worker_df_v1 = worker_df_v1.select(column_names)
    worker_df = worker_df_v0.unionByName(worker_df_v1)

    worker_df = clean(worker_df, column_names, schema)
    if since_date is not None:
        return worker_df.filter(F.col("import_date") > since_date)

    return worker_df


def clean(input_df, all_columns, schema):
    print("Cleaning...")

    should_be_integers = get_columns_that_should_be_integers(all_columns, schema)
    input_df = cast_column_to_type(input_df, should_be_integers, IntegerType())

    should_be_floats = get_columns_that_should_be_floats()
    input_df = cast_column_to_type(input_df, should_be_floats, FloatType())

    return input_df


def get_columns_that_should_be_integers(all_columns, schema):
    relevant_columns = []

    # TODO use function to extract these using regex patterns
    for column in all_columns:
        if ("year" in column) or ("flag" in column) or ("ql" in column):
            relevant_columns.append(column)

    training_related = utils.extract_col_with_pattern(
        r"^tr\d\d(count|ac|nac|dn)$", schema
    )
    others = ["emplstat", "zerohours", "salaryint"]

    return relevant_columns + training_related + others


def get_columns_that_should_be_floats():
    return [
        "distwrkk",
        "dayssick",
        "averagehours",
        "conthrs",
        "salary",
        "hrlyrate",
        "previous_pay",
    ]


def cast_column_to_type(input_df, columns, type):
    for column_name in columns:
        input_df = input_df.withColumn(column_name, input_df[column_name].cast(type))
    return input_df


def replace_columns_with_aggregated_column(
    df,
    col_name,
    udf_function,
    cols_to_aggregate=None,
    cols_to_remove=None,
    types=None,
    output_type=StringType(),
):
    df = add_aggregated_column(
        df, col_name, cols_to_aggregate, udf_function, types, output_type
    )
    if cols_to_remove:
        df = df.drop(*cols_to_remove)
    else:
        df = df.drop(*cols_to_aggregate)

    return df


def add_aggregated_column(
    df, col_name, columns, udf_function, types=None, output_type=StringType()
):
    curried_func = partial(udf_function, types=types)
    aggregate_udf = F.udf(curried_func, output_type)
    to_be_aggregated_df = df.select(columns)
    df = df.withColumn(
        col_name,
        aggregate_udf(
            F.struct([to_be_aggregated_df[x] for x in to_be_aggregated_df.columns])
        ),
    )

    return df


def get_training_into_json(row, types):
    aggregated_training = {}

    for training in types:
        if row[f"{training}flag"] == 1:
            aggregated_training[training] = {
                "latestdate": str(row[training + "latestdate"])[0:10],
                "count": row[training + "count"],
                "ac": row[training + "ac"],
                "nac": row[training + "nac"],
                "dn": row[training + "dn"],
            }

    return json.dumps(aggregated_training)


def get_job_role_into_json(row, types):
    agg_jr = []

    for jr in types:
        if row[jr] == 1:
            agg_jr.append(jr)

    return json.dumps(agg_jr)


def get_qualification_into_json(row, types):
    aggregated_qualifications = {}

    for qualification in types:
        if row[qualification] and (row[qualification] >= 1):
            aggregated_qualifications[qualification] = extract_qualification_info(
                row, qualification
            )

    return json.dumps(aggregated_qualifications)


def extract_year_column_name(qualification):
    capture_year = re.search(r"ql(\d+)[a-z]+", qualification)
    return f"ql{capture_year.group(1)}year"


def extract_qualification_info(row, qualification):
    if qualification == "ql34achqe":
        return {"count": row["ql34achqe"], "year": row["ql34yeare"]}

    if qualification[-1].isdigit():
        year = row[extract_year_column_name(qualification) + qualification[-1]]
    else:
        year = row[extract_year_column_name(qualification)]

    return {"count": row[qualification], "year": year}


def calculate_hours_worked(row, types=None):
    contracted_hrs = apply_sense_check_to_hrs_worked(row["conthrs"])
    average_hrs = apply_sense_check_to_hrs_worked(row["averagehours"])

    # employment status is permanent or temporary
    if row["emplstat"] in [190, 191]:

        # role is zero hours contract
        if row["zerohours"] == 1:
            if average_hrs:
                return average_hrs
        else:
            if contracted_hrs:
                return contracted_hrs

    # employment status is bank/pool, agency, hourly, other
    if row["emplstat"] in [192, 193, 194, 196]:
        if average_hrs:
            return average_hrs

    if contracted_hrs and contracted_hrs > 0:
        return contracted_hrs

    if average_hrs and average_hrs > 0:
        return average_hrs

    return None


def apply_sense_check_to_hrs_worked(hours):
    if hours in [None, -1, -2] or hours > 100:
        return None
    return hours


def calculate_hourly_pay(row, types=None):
    # salary is annual
    if (
        (row["salaryint"] == 250)
        and row["salary"]
        and row["hrs_worked"]
        and (row["hrs_worked"] > 0)
    ):
        return round(row["salary"] / 52 / row["hrs_worked"], 2)

    # salary is hourly
    if row["salaryint"] == 252:
        return row["hrlyrate"]

    return None


def collect_arguments():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--source",
        help="A CSV file or directory of files used as job input",
        required=True,
    )
    parser.add_argument(
        "--destination",
        help="A destination directory for outputting parquet files",
        required=True,
    )

    args, _ = parser.parse_known_args()

    return args.source, args.destination


if __name__ == "__main__":
    print("Spark job 'prepare_workers' starting...")
    print(f"Job parameters: {sys.argv}")

    source, destination = collect_arguments()
    main(source, WORKER_SCHEMA, destination)

    print("Spark job 'prepare_workers' complete")
