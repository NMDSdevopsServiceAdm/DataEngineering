import sys
import argparse
import pyspark.sql.functions as F

from schemas.capacity_tracker_schema import (
    CAPACITY_TRACKER_CARE_HOMES,
    CAPACITY_TRACKER_NON_RESIDENTIAL,
)
from utils import utils


def main(care_home_source, care_home_destination, non_res_source, non_res_destination):
    import_capacity_tracker_file(care_home_source, care_home_destination, CAPACITY_TRACKER_CARE_HOMES, "Last_Updated_UTC", "dd MMM yyyy")
    import_capacity_tracker_file(non_res_source, non_res_destination, CAPACITY_TRACKER_NON_RESIDENTIAL, "CQC_Survey_Last_Updated_UTC", "d/M/y")


def import_capacity_tracker_file (source, destination, schema, date_column_name, date_format):
    df = utils.read_csv_with_defined_schema(
        source, schema
    )
    df = add_column_with_formatted_dates(
        df,
        date_column_name,
        date_format,
    )
    utils.write_to_parquet(df, destination, False)


def add_column_with_formatted_dates(df, old_column, date_format):
    new_column = old_column + "_formatted"
    df = df.withColumn(new_column, F.trim(F.substring(F.col(old_column), 1, 11)))

    df_with_formatted_date = utils.format_date_fields(
        df,
        raw_date_format=date_format,
        date_column_identifier=new_column,
    )
    return df_with_formatted_date


def collect_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--care_home_source",
        help="A CSV file with capacity tracker data for care homes",
        required=True,
    )
    parser.add_argument(
        "--care_home_destination",
        help="A destination directory for outputting parquet files",
        required=True,
    )
    parser.add_argument(
        "--non_res_source",
        help="A CSV file with capacity tracker data for non residential",
        required=True,
    )
    parser.add_argument(
        "--non_res_destination",
        help="A destination directory for outputting parquet files",
        required=True,
    )

    args, _ = parser.parse_known_args()

    return (
        args.care_home_source,
        args.care_home_destination,
        args.non_res_source,
        args.non_res_destination,
    )


if __name__ == "__main__":
    print("Spark job 'ingest_capacity_tracker_data' starting...")
    print(f"Job parameters: {sys.argv}")

    (
        care_home_source,
        care_home_destination,
        non_res_source,
        non_res_destination,
    ) = collect_arguments()
    main(care_home_source, care_home_destination, non_res_source, non_res_destination)

    print("Spark job 'ingest_capacity_tracker_data' done")
