import argparse

from pyspark.sql.functions import col

from utils import utils
from environment import constants


def main(worker_source, ascwds_import_date):
    print("Estimating ethnicity")

    ascwds_ethnicity_df = get_ascwds_ethnicity_df(worker_source, ascwds_import_date)

    print(f"Exporting as parquet to {destination}")
    utils.write_to_parquet(ascwds_ethnicity_df, destination)


def get_ascwds_ethnicity_df(worker_source, ascwds_import_date, base_path=constants.ASCWDS_WORKER_BASE_PATH):
    spark = utils.get_spark()
    print(f"Reading workers parquet from {worker_source}")
    ethnicity_df = (
        spark.read.option("basePath", base_path)
        .parquet(worker_source)
        .filter(col("import_date") == ascwds_import_date)
        .filter(col("ethnicity") > -1)
        .filter(col("ethnicity") < 99)
        .select(col("locationid"), col("mainjrid"), col("ethnicity"))
    )

    return ethnicity_df


def collect_arguments():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--worker_source",
        help="Source s3 directory for ASCWDS worker dataset",
        required=True,
    )
    parser.add_argument(
        "--ascwds_import_date",
        help="The import date of ASCWDS data in the format yyyymmdd.",
        required=True,
    )
    parser.add_argument(
        "--destination",
        help="A destination directory for outputting ethnicity data, if not provided shall default to S3 todays date.",
        required=True,
    )

    args, unknown = parser.parse_known_args()

    return (
        args.worker_source,
        args.ascwds_import_date,
        args.destination,
    )


if __name__ == "__main__":
    (
        worker_source,
        ascwds_import_date,
        destination,
    ) = collect_arguments()
    main(worker_source, ascwds_import_date, destination)
