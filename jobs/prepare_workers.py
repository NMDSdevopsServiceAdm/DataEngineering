import argparse
import sys
import json

from pyspark.sql.functions import udf, struct
from pyspark.sql.types import StringType

from schemas import worker_schema
from schemas.worker_schema import WORKER_SCHEMA
from utils import utils


def main(source, destination):
    # TODO - read data as df
    main_df = get_dataset_worker(source)

    columns_to_be_aggregated_patterns = {
        "training":{
            "pattern": "^tr\d\d[a-z]",
            "udf_function": get_training_into_json
        },
        "job_role": {
            "pattern": "^jr\d\d[a-z]",
            "udf_function": get_job_role_into_json
        },
        "qualifications": {}
    }
    
    # TODO - replace training/jb/ql columns with aggregated columns
    for col_name, info in columns_to_be_aggregated_patterns.items():
        for pattern, udf_function in info.items():
            main_df = replace_columns_after_aggregation(main_df, col_name, pattern, udf_function)
    # main_df = replace_job_role_columns(main_df)


    # TODO - write the main df to destination
    return main_df


def get_dataset_worker(source):
    spark = utils.get_spark()
    column_names = utils.extract_column_from_schema(WORKER_SCHEMA)

    print(f"Reading worker parquet from {source}")
    worker_df = (
        spark.read.option("basePath", source).parquet(source).select(column_names)
    )

    return worker_df


def replace_columns_after_aggregation(df, col_name, pattern, udf_function):
    cols = utils.extract_col_with_pattern(
        pattern, worker_schema.WORKER_SCHEMA
    )
    df = add_aggregated_column(df, col_name, cols, udf_function)

    df = df.drop(struct(cols))

    return df


def add_aggregated_column(df, col_name, columns, udf_function):
    # TODO - can this be made more general so it will call both functions
    aggregate_udf = udf(udf_function, StringType())

    to_be_aggregated_df = df.select(columns)
    df = df.withColumn(
        col_name, aggregate_udf(struct([to_be_aggregated_df[x] for x in to_be_aggregated_df.columns]))
    )

    return df


def get_training_into_json(row):
    types_training = utils.extract_training_types(WORKER_SCHEMA)
    aggregated_training = {}

    for training in types_training:
        if row[f"{training}flag"] == 1:
            aggregated_training[training] = {
                "latestdate": row[training + "latestdate"],
                "count": row[training + "count"],
                "ac": row[training + "ac"],
                "nac": row[training + "nac"],
                "dn": row[training + "dn"],
            }

    return json.dumps(aggregated_training)


# def replace_job_role_columns(df):
#     job_role_cols = utils.extract_col_with_pattern(
#         "^jr\d\d[a-z]", worker_schema.WORKER_SCHEMA
#     )
#     df = add_aggregated_job_role_column(df, job_role_cols)

#     df = df.drop(struct(job_role_cols))

#     return df


# def add_aggregated_job_role_column(df, job_role_columns):
#     aggregate_jr_udf = udf(get_job_role_into_json, StringType())

#     jr_df = df.select(job_role_columns)
#     df = df.withColumn(
#         "job_role", aggregate_jr_udf(struct([jr_df[x] for x in jr_df.columns]))
#     )

    # return df

def get_job_role_into_json(row):
    job_role_cols = utils.extract_col_with_pattern(
        "^jr\d\d[a-z]", worker_schema.WORKER_SCHEMA
    )
    agg_jr = []
    for jr in job_role_cols:
        if row[jr] == 1:
            agg_jr.append(jr)

    return json.dumps(agg_jr)


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

    args, unknown = parser.parse_known_args()

    return args.source, args.destination


if __name__ == "__main__":
    print("Spark job 'prepare_workers' starting...")
    print(f"Job parameters: {sys.argv}")

    source, destination = collect_arguments()
    main(source, destination)

    print("Spark job 'prepare_workers' complete")
