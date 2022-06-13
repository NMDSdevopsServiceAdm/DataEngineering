import argparse
import sys
import re
import json

from schemas.worker_schema import WORKER_SCHEMA
from utils import utils


def main(source, destination):
    # TODO - read data as df
    main_df = get_dataset_worker(source)

    # TODO - extract training columns - they might need to be replace with only one col
    training_df = []

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


def replace_columns_with_aggregated_column(df, columns_df, aggregated_col):
    # TODO - drop columns
    
    # TODO - add the one column that has the aggregated values
    # df.withColumn(aggregated_col)
    # return df
    pass

def select_col_with_pattern(starting_pattern, df):
  pattern = re.compile(fr'^{starting_pattern}.')
  for col in df.columns:
    match = re.search(pattern, col)
    if not match:
      df = df.drop([col], axis=1)
  return df

def aggregate_training_columns(row):
    #TODO - func that will generate all types of training = 39
  types_training = ["tr01", "tr02"]
  aggregated_training = {}
  print(row)
  for training in types_training:
    print(row[f"{training}flag"])
    if row[f"{training}flag"] == 1:
      aggregated_training[training] = {
          'latestdate': row[training + "latestdate"],
          'count': row[training + "count"],
          'ac': row[training + "ac"],
          'nac': row[training + "nac"],
          'dn': row[training + "dn"]
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
