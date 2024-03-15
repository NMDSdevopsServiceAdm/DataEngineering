import argparse
from datetime import date

from utils import cqc_api as cqc
from utils import utils
from schemas.cqc_location_schema import LOCATION_SCHEMA
from utils.column_names.raw_data_files.cqc_location_api_columns import (
    CqcLocationApiColumns as ColNames,
)


def main(destination, aws_stored_partner_code):
    print("Collecting all locations from API")
    spark = utils.get_spark()
    df = None
    for paginated_locations in cqc.get_all_objects(
        stream=True,
        object_type="locations",
        object_identifier=ColNames.location_id,
        partner_code=aws_stored_partner_code,
    ):
        locations_df = spark.createDataFrame(paginated_locations, LOCATION_SCHEMA)
        if df:
            df = df.union(locations_df)
        else:
            df = locations_df

    df = df.dropDuplicates([ColNames.location_id])
    utils.write_to_parquet(df, destination, "append")

    print(f"Finished! Files can be found in {destination}")


if __name__ == "__main__":
    (destination_prefix, aws_stored_partner_code) = utils.collect_arguments(
        (
            "--destination_prefix",
            "Source s3 directory for parquet CQC providers dataset",
            False,
        ),
        (
            "--partner_code",
            "The partner code used for increasing the rate limit, called from AWS secrets",
            False,
        ),
    )
    todays_date = date.today()
    destination = utils.generate_s3_datasets_dir_date_path(
        destination_prefix=destination_prefix,
        domain="CQC",
        dataset="locations_api",
        date=todays_date,
    )

    print(destination)

    main(destination, aws_stored_partner_code)
