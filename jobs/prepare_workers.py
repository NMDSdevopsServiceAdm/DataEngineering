import argparse
import sys
import json
import re

from pyspark.sql.functions import udf, struct
from pyspark.sql.types import StringType, FloatType

from schemas.worker_schema import WORKER_SCHEMA
from utils import utils


def main(source, destination=None):
    main_df = get_dataset_worker(source)

    main_df = utils.format_import_date(main_df)
    main_df = utils.format_date_fields(main_df)

    columns_to_be_aggregated_patterns = {
        "training": {
            "pattern": "^tr\d\d[a-z]+",
            "udf_function": get_training_into_json,
        },
        "job_role": {
            "pattern": "^jr\d\d[a-z]+",
            "udf_function": get_job_role_into_json,
        },
        "qualifications": {
            "pattern": "^ql\d{1,3}[a-z]+.",
            "udf_function": get_qualification_into_json,
        },
    }
    for col_name, info in columns_to_be_aggregated_patterns.items():
        main_df = replace_columns_with_aggregated_column(
            main_df, col_name, info["udf_function"], pattern=info["pattern"]
        )

    # get hours worked
    main_df = replace_columns_with_aggregated_column(
        main_df,
        "hrs_worked",
        calculate_hours_worked,
        cols_to_aggregate=["emplstat", "zerohours", "averagehours", "conthrs"],
        cols_to_remove=["averagehours", "conthrs"],
        output_type=FloatType(),
    )

    # add salary per hour
    main_df = replace_columns_with_aggregated_column(
        main_df,
        "hourly_rate",
        calculate_hourly_pay,
        cols_to_aggregate=["salary", "salaryint", "hrlyrate", "hrs_worked"],
        cols_to_remove=["salary", "hrlyrate"],
        output_type=FloatType(),
    )

    if destination:
        print(f"Exporting as parquet to {destination}")
        utils.write_to_parquet(main_df, destination)
    else:
        return main_df


def get_dataset_worker(source):
    spark = utils.get_spark()
    column_names = utils.extract_column_from_schema(WORKER_SCHEMA)

    print(f"Reading worker parquet from {source}")
    worker_df = (
        spark.read.option("basePath", source).parquet(source).select(column_names)
    )

    return worker_df


def replace_columns_with_aggregated_column(
    df,
    col_name,
    udf_function,
    pattern=None,
    cols_to_aggregate=None,
    cols_to_remove=None,
    output_type=StringType(),
):
    if pattern:
        cols_to_aggregate = utils.extract_col_with_pattern(pattern, WORKER_SCHEMA)

    df = add_aggregated_column(
        df, col_name, cols_to_aggregate, udf_function, output_type
    )
    if cols_to_remove:
        df = df.drop(*cols_to_remove)
    else:
        df = df.drop(*cols_to_aggregate)

    return df


def add_aggregated_column(
    df, col_name, columns, udf_function, output_type=StringType()
):
    aggregate_udf = udf(udf_function, output_type)

    to_be_aggregated_df = df.select(columns)
    df = df.withColumn(
        col_name,
        aggregate_udf(
            struct([to_be_aggregated_df[x] for x in to_be_aggregated_df.columns])
        ),
    )

    return df


def get_training_into_json(row):
    types_training = utils.extract_specific_column_types("^tr\d\dflag$", WORKER_SCHEMA)
    aggregated_training = {}

    for training in types_training:
        if row[f"{training}flag"] == 1:
            aggregated_training[training] = {
                "latestdate": str(row[training + "latestdate"])[0:10],
                "count": row[training + "count"],
                "ac": row[training + "ac"],
                "nac": row[training + "nac"],
                "dn": row[training + "dn"],
            }

    return json.dumps(aggregated_training)


def get_job_role_into_json(row):
    job_role_cols = utils.extract_col_with_pattern("^jr\d\d[a-z]", WORKER_SCHEMA)
    agg_jr = []
    for jr in job_role_cols:
        if row[jr] == 1:
            agg_jr.append(jr)

    return json.dumps(agg_jr)


def get_qualification_into_json(row):
    qualification_types = utils.extract_col_with_pattern(
        "^ql\d{1,3}(achq|app)(\d*|e)", WORKER_SCHEMA
    )
    aggregated_qualifications = {}

    for qualification in qualification_types:
        if row[qualification] >= 1:
            aggregated_qualifications[qualification] = extract_qualification_info(
                row, qualification
            )

    return json.dumps(aggregated_qualifications)


def extract_year_column_name(qualification):
    capture_year = re.search(r"ql(\d+)[a-z]+", qualification)
    return f"ql{capture_year.group(1)}year"


def extract_qualification_info(row, qualification):
    if qualification == "ql34achqe":
        return {"value": row["ql34achqe"], "year": row["ql34yeare"]}

    if qualification[-1].isdigit():
        year = row[extract_year_column_name(qualification) + qualification[-1]]

    else:
        year = row[extract_year_column_name(qualification)]

    return {"value": row[qualification], "year": year}


def calculate_hours_worked(row):
    cHrs = row["conthrs"]
    aHrs = row["averagehours"]

    if cHrs in [None, -1, -2] or cHrs > 100:
        cHrs = None

    if aHrs in [None, -1, -2] or aHrs > 100:
        aHrs = None

    # Role is perm or temp
    if row["emplstat"] in ["Permanent", "Temporary"]:
        # role is zero hr
        if row["zerohours"] == "Yes":
            if not aHrs:
                if not cHrs:
                    return cHrs
                return aHrs
            return aHrs
        # role is NOT zero hr
        if row["zerohours"] != "Yes":
            if not cHrs:
                if not aHrs:
                    return aHrs
                return cHrs
            return cHrs
    # If role not perm or temp
    else:
        if not aHrs:
            if not cHrs:
                return cHrs
            return aHrs
        return aHrs


def calculate_hourly_pay(row):
    if row["salaryint"] == 250:
        if row["salary"]:
            try:
                return row["salary"] / 52 / row["hrs_worked"]
            except:
                return None

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
    main(source, destination)

    print("Spark job 'prepare_workers' complete")
