import json
from datetime import date

from projects._01_ingest.cqc_api.utils import cqc_api as cqc
from utils import aws_secrets_manager_utilities as ars
from utils import utils
from schemas.cqc_location_schema import LOCATION_SCHEMA
from utils.column_names.raw_data_files.cqc_location_api_columns import (
    NewCqcLocationApiColumns as ColNames,
)


def main(destination):
    print("Collecting all locations from API")
    spark = utils.get_spark()
    df = None
    cqc_api_primary_key_value = json.loads(
        ars.get_secret(secret_name="cqc_api_primary_key", region_name="eu-west-2")
    )["Ocp-Apim-Subscription-Key"]
    for paginated_locations in cqc.get_all_objects(
        object_type="locations",
        object_identifier=ColNames.location_id,
        cqc_api_primary_key=cqc_api_primary_key_value,
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
        version="2.1.1",
    )

    print(destination)

    main(destination)
