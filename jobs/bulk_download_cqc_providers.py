import argparse
from datetime import date

from schemas.cqc_provider_schema import PROVIDER_SCHEMA
from utils import cqc_api as cqc
from utils import utils


def main(destination):
    print("Collecting all providers from API")
    spark = utils.get_spark()
    df = None
    for paginated_providers in cqc.get_all_objects(
        stream=True, object_type="providers", object_identifier="providerId"
    ):
        providers_df = spark.createDataFrame(paginated_providers, PROVIDER_SCHEMA)
        if df:
            df = df.union(providers_df)
        else:
            df = providers_df

    df = df.dropDuplicates(["providerId"])
    utils.write_to_parquet(df, destination, True)

    print(f"Finished! Files can be found in {destination}")


def collect_arguments():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--destinationprefix",
        help="A destination bucket name in the format of s3://<bucket_name>/",
        required=False,
    )

    args, _ = parser.parse_known_args()

    return args.destinationprefix


if __name__ == "__main__":
    destination_prefix = collect_arguments()
    todays_date = date.today()
    destination = utils.generate_s3_main_datasets_dir_date_path(
        destination_prefix=destination_prefix,
        domain="CQC",
        dataset="providers-api",
        date=todays_date,
    )

    main(destination)
