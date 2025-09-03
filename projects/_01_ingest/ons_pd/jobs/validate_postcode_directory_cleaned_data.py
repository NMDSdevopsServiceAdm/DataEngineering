import os
import sys

os.environ["SPARK_VERSION"] = "3.3"

from pyspark.sql.dataframe import DataFrame

from utils import utils
from utils.column_names.raw_data_files.ons_columns import (
    OnsPostcodeDirectoryColumns as ONS,
)
from utils.column_names.ind_cqc_pipeline_columns import (
    PartitionKeys as Keys,
)
from utils.validation.validation_rules.postcode_directory_cleaned_validation_rules import (
    PostcodeDirectoryCleanedValidationRules as Rules,
)
from utils.validation.validation_utils import (
    validate_dataset,
    raise_exception_if_any_checks_failed,
)
from utils.validation.validation_rule_names import RuleNames as RuleName

PartitionKeys = [Keys.year, Keys.month, Keys.day, Keys.import_date]

raw_postcode_directory_columns_to_import = [
    Keys.import_date,
    ONS.postcode,
]


def main(
    raw_postcode_directory_source: str,
    cleaned_postcode_directory_source: str,
    report_destination: str,
):
    raw_postcode_directory_df = utils.read_from_parquet(
        raw_postcode_directory_source,
        selected_columns=raw_postcode_directory_columns_to_import,
    )
    cleaned_postcode_directory_df = utils.read_from_parquet(
        cleaned_postcode_directory_source,
    )
    rules = Rules.rules_to_check

    rules[RuleName.size_of_dataset] = (
        calculate_expected_size_of_cleaned_postcode_directory_dataset(
            raw_postcode_directory_df
        )
    )

    check_result_df = validate_dataset(cleaned_postcode_directory_df, rules)

    utils.write_to_parquet(check_result_df, report_destination, mode="overwrite")

    if isinstance(check_result_df, DataFrame):
        raise_exception_if_any_checks_failed(check_result_df)


def calculate_expected_size_of_cleaned_postcode_directory_dataset(
    raw_postcode_directory_df: DataFrame,
) -> int:
    expected_size = raw_postcode_directory_df.count()
    return expected_size


if __name__ == "__main__":
    print("Spark job 'validate_postcode_directory_cleaned_data' starting...")
    print(f"Job parameters: {sys.argv}")

    (
        raw_postcode_directory_source,
        cleaned_postcode_directory_source,
        report_destination,
    ) = utils.collect_arguments(
        (
            "--raw_postcode_directory_source",
            "Source s3 directory for parquet postcode directory dataset",
        ),
        (
            "--cleaned_postcode_directory_source",
            "Source s3 directory for parquet postcode directory cleaned dataset",
        ),
        (
            "--report_destination",
            "Destination s3 directory for validation report parquet",
        ),
    )
    try:
        main(
            raw_postcode_directory_source,
            cleaned_postcode_directory_source,
            report_destination,
        )
    finally:
        spark = utils.get_spark()
        if spark.sparkContext._gateway:
            spark.sparkContext._gateway.shutdown_callback_server()
        spark.stop()

    print("Spark job 'validate_postcode_directory_cleaned_data' complete")
