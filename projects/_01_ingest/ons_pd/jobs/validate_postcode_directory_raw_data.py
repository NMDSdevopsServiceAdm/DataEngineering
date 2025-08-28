import os
import sys

os.environ["SPARK_VERSION"] = "3.3"

from pyspark.sql.dataframe import DataFrame

from utils import utils
from utils.validation.validation_rules.postcode_directory_raw_validation_rules import (
    PostcodeDirectoryRawValidationRules as Rules,
)
from utils.validation.validation_utils import (
    validate_dataset,
    raise_exception_if_any_checks_failed,
)


def main(
    raw_postcode_directory_source: str,
    report_destination: str,
):
    raw_postcode_directory_df = utils.read_from_parquet(
        raw_postcode_directory_source,
    )
    rules = Rules.rules_to_check

    check_result_df = validate_dataset(raw_postcode_directory_df, rules)

    utils.write_to_parquet(check_result_df, report_destination, mode="overwrite")

    if isinstance(check_result_df, DataFrame):
        raise_exception_if_any_checks_failed(check_result_df)


if __name__ == "__main__":
    print("Spark job 'validate_postcode_directory_raw_data' starting...")
    print(f"Job parameters: {sys.argv}")

    (
        raw_postcode_directory_source,
        report_destination,
    ) = utils.collect_arguments(
        (
            "--raw_postcode_directory_source",
            "Source s3 directory for parquet postcode directory dataset",
        ),
        (
            "--report_destination",
            "Destination s3 directory for validation report parquet",
        ),
    )
    try:
        main(
            raw_postcode_directory_source,
            report_destination,
        )
    finally:
        spark = utils.get_spark()
        if spark.sparkContext._gateway:
            spark.sparkContext._gateway.shutdown_callback_server()
        spark.stop()

    print("Spark job 'validate_postcode_directory_raw_data' complete")
