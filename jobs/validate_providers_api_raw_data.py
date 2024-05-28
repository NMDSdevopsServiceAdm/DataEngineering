import os
import sys

os.environ["SPARK_VERSION"] = "3.3"

from utils import utils
from utils.validation.validation_rules.providers_api_raw_validation_rules import (
    ProvidersAPIRawValidationRules as Rules,
)
from utils.validation.validation_utils import validate_dataset


def main(
    raw_cqc_provider_source: str,
    report_destination: str,
):
    raw_provider_df = utils.read_from_parquet(
        raw_cqc_provider_source,
    )
    rules = Rules.rules_to_check

    check_result_df = validate_dataset(raw_provider_df, rules)

    utils.write_to_parquet(check_result_df, report_destination, mode="overwrite")


if __name__ == "__main__":
    print("Spark job 'validate_providers_api_raw_data' starting...")
    print(f"Job parameters: {sys.argv}")

    (
        raw_cqc_provider_source,
        report_destination,
    ) = utils.collect_arguments(
        (
            "--raw_cqc_provider_source",
            "Source s3 directory for parquet providers api dataset",
        ),
        (
            "--report_destination",
            "Destination s3 directory for validation report parquet",
        ),
    )
    try:
        main(
            raw_cqc_provider_source,
            report_destination,
        )
    finally:
        spark = utils.get_spark()
        if spark.sparkContext._gateway:
            spark.sparkContext._gateway.shutdown_callback_server()
        spark.stop()

    print("Spark job 'validate_providers_api_raw_data' complete")
