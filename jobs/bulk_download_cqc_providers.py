import argparse
from datetime import date

from schemas.cqc_provider_schema import PROVIDER_SCHEMA
from utils import cqc_api as cqc
from utils import utils
from utils.column_names.raw_data_files.cqc_provider_api_columns import (
    CqcProviderApiColumns as ColNames,
)


def main(destination, aws_stored_partner_code):
    print("Collecting all providers from API")
    spark = utils.get_spark()
    df = None
    for paginated_providers in cqc.get_all_objects(
        stream=True,
        object_type="providers",
        object_identifier=ColNames.provider_id,
        partner_code=aws_stored_partner_code,
    ):
        providers_df = spark.createDataFrame(paginated_providers, PROVIDER_SCHEMA)
        if df:
            df = df.union(providers_df)
        else:
            df = providers_df

    df = df.dropDuplicates([ColNames.provider_id])
    utils.write_to_parquet(df, destination, "append")

    print(f"Finished! Files can be found in {destination}")


if __name__ == "__main__":
    destination_prefix, aws_stored_partner_code = utils.collect_arguments(
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
        dataset="providers_api",
        date=todays_date,
    )

    main(destination, aws_stored_partner_code)
