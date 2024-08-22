import os
import sys

os.environ["SPARK_VERSION"] = "3.3"

from pyspark.sql.dataframe import DataFrame

from jobs.clean_ind_cqc_filled_posts import reduce_dataset_to_earliest_file_per_month
from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import (
    PartitionKeys as Keys,
)
from utils.validation.validation_rules.cleaned_ind_cqc_validation_rules import (
    CleanedIndCqcValidationRules as Rules,
)
from utils.validation.validation_utils import (
    validate_dataset,
    raise_exception_if_any_checks_failed,
)
from utils.validation.validation_rule_names import RuleNames as RuleName

PartitionKeys = [Keys.year, Keys.month, Keys.day, Keys.import_date]


def main(
    merged_ind_cqc_source: str,
    cleaned_ind_cqc_source: str,
    report_destination: str,
):
    merged_ind_cqc_df = utils.read_from_parquet(
        merged_ind_cqc_source,
    )
    cleaned_ind_cqc_df = utils.read_from_parquet(
        cleaned_ind_cqc_source,
    )
    rules = Rules.rules_to_check

    rules[
        RuleName.size_of_dataset
    ] = calculate_expected_size_of_cleaned_ind_cqc_dataset(merged_ind_cqc_df)

    check_result_df = validate_dataset(cleaned_ind_cqc_df, rules)

    utils.write_to_parquet(check_result_df, report_destination, mode="overwrite")

    if isinstance(check_result_df, DataFrame):
        raise_exception_if_any_checks_failed(check_result_df)


def calculate_expected_size_of_cleaned_ind_cqc_dataset(
    merged_ind_cqc_df: DataFrame,
) -> int:
    expected_size = reduce_dataset_to_earliest_file_per_month(merged_ind_cqc_df).count()
    return expected_size


if __name__ == "__main__":
    print("Spark job 'validate_clened_ind_cqc_data' starting...")
    print(f"Job parameters: {sys.argv}")

    (
        merged_ind_cqc_source,
        cleaned_ind_cqc_source,
        report_destination,
    ) = utils.collect_arguments(
        (
            "--merged_ind_cqc_source",
            "Source s3 directory for parquet merged independent CQC dataset",
        ),
        (
            "--cleaned_ind_cqc_source",
            "Source s3 directory for parquet cleaned independent CQC dataset",
        ),
        (
            "--report_destination",
            "Destination s3 directory for validation report parquet",
        ),
    )
    try:
        main(
            merged_ind_cqc_source,
            cleaned_ind_cqc_source,
            report_destination,
        )
    finally:
        spark = utils.get_spark()
        if spark.sparkContext._gateway:
            spark.sparkContext._gateway.shutdown_callback_server()
        spark.stop()

    print("Spark job 'validate_cleaned_ind_cqc_data' complete")
