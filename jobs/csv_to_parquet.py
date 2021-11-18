from pyspark.sql import SparkSession
from pyspark.context import SparkContext
import sys
import pyspark
import argparse


def main(source, destination):
    spark = SparkSession.builder \
        .appName("sfc_data_engineering_csv_to_parquet") \
        .getOrCreate()

    df = spark.read.csv(source, header=True)
    df.write.parquet(destination)


def collect_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", help="A CSV file used as source input")
    parser.add_argument(
        "--destination", help="A destination directory for outputting parquet files")
    args, unknown = parser.parse_known_args()

    if not any([args.source, args.destination]):
        raise ValueError("Please provide a --source and --destination")

    return args.source, args.destination


if __name__ == '__main__':
    print("Spark job 'csv_to_parquet' starting...")
    print(f"Job parameters: {sys.argv}")

    source, destination = collect_arguments()
    main(source, destination)

    print("Spark job 'csv_to_parquet' done")
