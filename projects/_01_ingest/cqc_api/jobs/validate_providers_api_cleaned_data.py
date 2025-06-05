import os
import sys

os.environ["SPARK_VERSION"] = "3.3"

from pyspark.sql.dataframe import DataFrame

from utils import utils
from utils.column_names.raw_data_files.cqc_provider_api_columns import (
    CqcProviderApiColumns as CQCP,
)
from utils.column_names.ind_cqc_pipeline_columns import (
    PartitionKeys as Keys,
)
from utils.validation.validation_rules.providers_api_cleaned_validation_rules import (
    ProvidersAPICleanedValidationRules as Rules,
)
from utils.validation.validation_utils import (
    validate_dataset,
    add_column_with_length_of_string,
    raise_exception_if_any_checks_failed,
)
from utils.validation.validation_rule_names import RuleNames as RuleName

PartitionKeys = [Keys.year, Keys.month, Keys.day, Keys.import_date]

raw_cqc_providers_columns_to_import = [
    Keys.import_date,
    CQCP.provider_id,
]


def main(
    raw_cqc_provider_source: str,
    cleaned_cqc_providers_source: str,
    report_destination: str,
):
    raw_provider_df = utils.read_from_parquet(
        raw_cqc_provider_source,
        selected_columns=raw_cqc_providers_columns_to_import,
    )
    cleaned_cqc_providers_df = utils.read_from_parquet(
        cleaned_cqc_providers_source,
    )
    rules = Rules.rules_to_check

    rules[
        RuleName.size_of_dataset
    ] = calculate_expected_size_of_cleaned_cqc_providers_dataset(raw_provider_df)
    cleaned_cqc_providers_df = add_column_with_length_of_string(
        cleaned_cqc_providers_df, [CQCP.provider_id]
    )

    check_result_df = validate_dataset(cleaned_cqc_providers_df, rules)

    utils.write_to_parquet(check_result_df, report_destination, mode="overwrite")

    if isinstance(check_result_df, DataFrame):
        raise_exception_if_any_checks_failed(check_result_df)


def calculate_expected_size_of_cleaned_cqc_providers_dataset(
    raw_provider_df: DataFrame,
) -> int:
    expected_size = raw_provider_df.count()
    return expected_size


if __name__ == "__main__":
    print("Spark job 'validate_providers_api_cleaned_data' starting...")
    print(f"Job parameters: {sys.argv}")

    (
        raw_cqc_provider_source,
        cleaned_cqc_providers_source,
        report_destination,
    ) = utils.collect_arguments(
        (
            "--raw_cqc_provider_source",
            "Source s3 directory for parquet providers api dataset",
        ),
        (
            "--cleaned_cqc_providers_source",
            "Source s3 directory for parquet providers api cleaned dataset",
        ),
        (
            "--report_destination",
            "Destination s3 directory for validation report parquet",
        ),
    )
    try:
        main(
            raw_cqc_provider_source,
            cleaned_cqc_providers_source,
            report_destination,
        )
    finally:
        spark = utils.get_spark()
        if spark.sparkContext._gateway:
            spark.sparkContext._gateway.shutdown_callback_server()
        spark.stop()

    print("Spark job 'validate_providers_api_cleaned_data' complete")
