import argparse
import sys

from pyspark.sql.functions import col

from utils import utils


def main(source, destination):
    return True


def get_dataset_worker(source):
    spark = utils.get_spark()

    print(f"Reading worker parquet from {source}")
    worker_df = (
        spark.read.option("basePath", source)
        .parquet(source)
        .select(
            col("period"),
            col("establishmentid")
        )
    )
    return worker_df


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
