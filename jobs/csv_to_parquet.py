from pyspark.sql import SparkSession
from pyspark.context import SparkContext
import sys
import pyspark
import argparse

DEFAULT_DELIMITER = ","


def main(source, destination, delimiter):
    df = read_csv(source, delimiter)
    write_parquet(df, destination)


def read_csv(source, delimiter=DEFAULT_DELIMITER):
    spark = SparkSession.builder \
        .appName("sfc_data_engineering_csv_to_parquet") \
        .getOrCreate()

    df = spark.read.option("delimiter", delimiter).csv(source, header=True)

    return df


def write_parquet(df, destination):
    df.write.parquet(destination)


def collect_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--source", help="A CSV file used as source input", required=True)
    parser.add_argument(
        "--destination", help="A destination directory for outputting parquet files", required=True)
    parser.add_argument(
        "--delimiter", help="Specify a custom field delimiter", required=False, default=",")

    args, unknown = parser.parse_known_args()

    if args.delimiter:
        print(f"Utilising custom delimiter '{args.delimiter}'")

    return args.source, args.destination, args.delimiter


if __name__ == '__main__':
    print("Spark job 'csv_to_parquet' starting...")
    print(f"Job parameters: {sys.argv}")

    source, destination, delimiter = collect_arguments()
    main(source, destination, delimiter)

    print("Spark job 'csv_to_parquet' done")
