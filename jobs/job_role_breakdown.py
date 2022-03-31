"""
Return a list of all cqc locations with a breakdown of job role counts for 2021

Columns: Locationid, jobroleid, jobrole name, count of workers
"""

import argparse
from datetime import date

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import abs, coalesce, greatest, lit, max, when, col, to_date, lower
from pyspark.sql.types import IntegerType


def main(destinaton):
    print("Determining job role breakdown for cqc locations")

    print(f"Exporting as parquet to {destination}")
    utils.write_to_parquet(output_df, destination)


def collect_arguments():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--destination",
        help="A destination directory for outputting job role breakdown",
        required=True,
    )

    args, unknown = parser.parse_known_args()

    return (
        args.destination,
    )


if __name__ == "__main__":
    (
        destination,
    ) = collect_arguments()
    main(destination)
