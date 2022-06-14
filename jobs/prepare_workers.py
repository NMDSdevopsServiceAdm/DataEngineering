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

    # TODO - extract training columns - they might need to be replace with only one col
    tr_df = []

    # TODO - get training column and replace in main
    aggregated_column = get_training_aggregated_column(tr_df)
    main_df = replace_training_columns(main_df, aggregated_column)

    # TODO - write the main df somewhere
    return main_df


def get_dataset_worker(source):
    spark = utils.get_spark()
    column_names = utils.extract_column_from_schema(WORKER_SCHEMA)

    print(f"Reading worker parquet from {source}")
    worker_df = (
        spark.read.option("basePath", source).parquet(source).select(column_names)
    )

    return worker_df


def replace_training_columns(df):
    training_cols = utils.extract_col_with_pattern("^tr\d\d[a-z]", worker_schema.WORKER_SCHEMA)
    df = df.drop(struct(training_cols))
    # df = df.withColumn("training", aggregated_column)
    return df


def get_training_aggregated_column(tr_df):
    aggregate_training_udf = udf(get_training_into_json, StringType())
    tr_df = tr_df.withColumn('training', aggregate_training_udf(struct([tr_df[x] for x in tr_df.columns])))
    return tr_df.select("training")


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
