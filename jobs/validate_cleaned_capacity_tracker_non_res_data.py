import os
import sys

os.environ["SPARK_VERSION"] = "3.3"

from pyspark.sql import DataFrame

from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import (
    PartitionKeys as Keys,
)
from utils.validation.validation_rules.cleaned_capacity_tracker_non_res_validation_rules import (
    CleanedCapacityTrackerNonResValidationRules as Rules,
)
from utils.validation.validation_utils import (
    validate_dataset,
    raise_exception_if_any_checks_failed,
)
from utils.validation.validation_rule_names import RuleNames as RuleName

PartitionKeys = [Keys.year, Keys.month, Keys.day, Keys.import_date]


def main(
    capacity_tracker_non_res_source: str,
    capacity_tracker_non_res_cleaned_source: str,
    report_destination: str,
):
    non_res_df = utils.read_from_parquet(
        capacity_tracker_non_res_source,
    )
    non_res_cleaned_df = utils.read_from_parquet(
        capacity_tracker_non_res_cleaned_source,
    )
    rules = Rules.rules_to_check

    rules[RuleName.size_of_dataset] = (
        calculate_expected_size_of_cleaned_capacity_tracker_non_res_dataset(non_res_df)
    )

    check_result_df = validate_dataset(non_res_cleaned_df, rules)

    utils.write_to_parquet(check_result_df, report_destination, mode="overwrite")

    if isinstance(check_result_df, DataFrame):
        raise_exception_if_any_checks_failed(check_result_df)


def calculate_expected_size_of_cleaned_capacity_tracker_non_res_dataset(
    non_res_df: DataFrame,
) -> int:
    expected_size = non_res_df.count()
    return expected_size


if __name__ == "__main__":
    print("Spark job 'validate_cleaned_capacity_tracker_non_res_data' starting...")
    print(f"Job parameters: {sys.argv}")

    (
        capacity_tracker_non_res_source,
        capacity_tracker_non_res_cleaned_source,
        report_destination,
    ) = utils.collect_arguments(
        (
            "--capacity_tracker_non_res_source",
            "Source s3 directory for parquet capacity tracker non residential dataset",
        ),
        (
            "--capacity_tracker_non_res_cleaned_source",
            "Source s3 directory for parquet cleaned capacity tracker non residential dataset",
        ),
        (
            "--report_destination",
            "Destination s3 directory for validation report parquet",
        ),
    )
    try:
        main(
            capacity_tracker_non_res_source,
            capacity_tracker_non_res_cleaned_source,
            report_destination,
        )
    finally:
        spark = utils.get_spark()
        if spark.sparkContext._gateway:
            spark.sparkContext._gateway.shutdown_callback_server()
        spark.stop()

    print("Spark job 'validate_cleaned_capacity_tracker_non_res_data' complete")
