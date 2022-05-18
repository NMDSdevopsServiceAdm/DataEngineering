# STEPS
#
# DONE - import 2 provider files 12 months apart
# DONE - filter both to those updating within 6 months
# TODO - inner join (maybe?) to identify estids in both files
# TODO - import majority of worker data from 'early' file
# TODO - import selected data from later file

import argparse
from pyspark.sql.functions import col, add_months
from jobs import prepare_locations

from utils import utils

START_PERIOD_WORKPLACE_FILE = "s3://sfc-data-engineering/domain=ASCWDS/dataset=workplace/version=0.0.1/year=2021/month=03/day=31/import_date=20210331/"
START_PERIOD_WORKER_FILE = "s3://sfc-data-engineering/domain=ASCWDS/dataset=worker/version=0.0.1/year=2021/month=03/day=31/import_date=20210331/"

END_PERIOD_WORKPLACE_FILE = "s3://sfc-data-engineering/domain=ASCWDS/dataset=workplace/version=1.0.0/year=2022/month=04/day=01/import_date=20220401/"
END_PERIOD_WORKER_FILE = "s3://sfc-data-engineering/domain=ASCWDS/dataset=worker/version=1.0.0/year=2022/month=04/day=01/import_date=20220401/"


def main(
    source_start_workplace_file=START_PERIOD_WORKPLACE_FILE,
    source_end_workplace_file=END_PERIOD_WORKPLACE_FILE,
    destination=None,
):

    print("Creating stayer vs leaver parquet file")
    start_workplace_df = updated_within_time_period(source_start_workplace_file)

    end_workplace_df = updated_within_time_period(source_end_workplace_file)

    starters_vs_leavers_df = workplaces_in_both_dfs(start_workplace_df, end_workplace_df)

    if destination:
        print(f"Exporting as parquet to {destination}")
        utils.write_to_parquet(starters_vs_leavers_df, destination)
    else:
        return starters_vs_leavers_df


def updated_within_time_period(df):
    spark = utils.get_spark()

    df = spark.read.parquet(df)
    df = df.select("establishmentid", "mupddate", "import_date")
    df = prepare_locations.format_import_date(df)
    df = df.withColumn("mupddate_cutoff", add_months(df.import_date, -6))
    df = df.filter(df.mupddate > df.mupddate_cutoff)
    df = df.select("establishmentid")

    return df


def workplaces_in_both_dfs(start_workplace_df, end_workplace_df):

    df = start_workplace_df.join(end_workplace_df, ["establishmentid"], "inner")

    return df


def collect_arguments():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--source_start_workplace_file",
        help="Source s3 directory for ASCWDS workplace dataset - start of 12 month period.",
        required=True,
    )
    parser.add_argument(
        "--source_end_workplace_file",
        help="Source s3 directory for ASCWDS workplace dataset - end of 12 month period.",
        required=True,
    )
    parser.add_argument(
        "--destination",
        help="A destination directory for outputting cqc locations, if not provided shall default to S3 todays date.",
        required=False,
    )

    args, unknown = parser.parse_known_args()

    return args.source_start_workplace_file, args.source_end_workplace_file, args.destination


if __name__ == "__main__":
    (source_start_workplace_file, source_end_workplace_file, destination) = collect_arguments()

    main(source_start_workplace_file, source_end_workplace_file, destination)
