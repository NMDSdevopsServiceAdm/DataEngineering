# STEPS
#
# import 2 provider files 12 months apart
# filter both to those updating within 6 months
# inner join (maybe?) to identify estids in both files
# import majority of worker data from 'early' file
# import selected data from later file

import argparse

from utils import utils

START_PERIOD_WORKPLACE_FILE = "s3://sfc-data-engineering/domain=ASCWDS/dataset=workplace/version=0.0.1/year=2021/month=03/day=31/import_date=20210331/"
START_PERIOD_WORKER_FILE = "s3://sfc-data-engineering/domain=ASCWDS/dataset=worker/version=0.0.1/year=2021/month=03/day=31/import_date=20210331/"

END_PERIOD_WORKPLACE__FILE = "s3://sfc-data-engineering/domain=ASCWDS/dataset=workplace/version=1.0.0/year=2022/month=04/day=01/import_date=20220401/"
END_PERIOD_WORKER_FILE = "s3://sfc-data-engineering/domain=ASCWDS/dataset=worker/version=1.0.0/year=2022/month=04/day=01/import_date=20220401/"


def main(destination=None):
    spark = utils.get_spark()

    print("Creating stayer vs leaver parquet file")

    starters_vs_leavers_df = None

    print(f"Exporting as parquet to {destination}")
    utils.write_to_parquet(starters_vs_leavers_df, destination)


def collect_arguments():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--destination",
        help="A destination directory for outputting cqc locations, if not provided shall default to S3 todays date.",
        required=False,
    )

    args, unknown = parser.parse_known_args()

    return args.destination


if __name__ == "__main__":
    (destination) = collect_arguments()

    main(destination)
