import sys
import argparse
import pyspark.sql.functions as F

from projects._01_ingest.utils.utils import ingest_utils
from schemas.spss_job_estimates_schema import SPSS_JOBS_ESTIMATES
from utils import utils


def main(source, destination):
    df = ingest_utils.read_csv_with_defined_schema(source, SPSS_JOBS_ESTIMATES)
    df_with_formatted_date = df.withColumn(
        "snapshot_date_formatted", F.col("Snapshot_date")
    )
    df_with_formatted_date = utils.format_date_fields(
        df_with_formatted_date,
        raw_date_format="yyyyMMdd",
        date_column_identifier="snapshot_date_formatted",
    )
    utils.write_to_parquet(df_with_formatted_date, destination)


def collect_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--source", help="A CSV file used as source input", required=True
    )
    parser.add_argument(
        "--destination",
        help="A destination directory for outputting parquet files",
        required=True,
    )

    args, _ = parser.parse_known_args()

    return args.source, args.destination


if __name__ == "__main__":
    print("Spark job 'csv_to_parquet' starting...")
    print(f"Job parameters: {sys.argv}")

    source, destination = collect_arguments()
    main(source, destination)

    print("Spark job 'spss_csv_to_parquet' done")
