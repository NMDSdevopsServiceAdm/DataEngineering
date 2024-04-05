from datetime import date

from utils import cqc_api_new as cqc
from utils import aws_secrets_manager_utilities as ars
from utils import utils
from schemas.cqc_location_schema import LOCATION_SCHEMA_NEW
from utils.column_names.raw_data_files.cqc_location_api_columns import (
    CqcLocationApiColumns as ColNames,
)

import json


def main(destination):
    print("Collecting all locations from API")
    spark = utils.get_spark()
    df = None
    partner_code_value = json.loads(
        ars.get_secret(secret_name="partner_code", region_name="eu-west-2")
    )["partner_code"]
    for paginated_locations in cqc.get_all_objects(
        object_type="locations",
        object_identifier=ColNames.location_id,
        partner_code=partner_code_value,
    ):
        locations_df = spark.createDataFrame(paginated_locations, LOCATION_SCHEMA_NEW)
        if df:
            df = df.union(locations_df)
        else:
            df = locations_df

    df = df.dropDuplicates([ColNames.location_id])
    utils.write_to_parquet(df, destination, "append")

    print(f"Finished! Files can be found in {destination}")


if __name__ == "__main__":
    destination_prefix, *_ = utils.collect_arguments(
        (
            "--destination_prefix",
            "Source s3 directory for parquet CQC locations dataset",
            False,
        ),
    )
    todays_date = date.today()
    destination = utils.generate_s3_datasets_dir_date_path(
        destination_prefix=destination_prefix,
        domain="CQC",
        dataset="locations_api",
        date=todays_date,
        version="2.0.0",
    )

    print(destination)

    main(destination)
