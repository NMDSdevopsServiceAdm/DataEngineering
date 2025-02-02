import os
import sys

os.environ["SPARK_VERSION"] = "3.3"

from pyspark.sql import DataFrame

from utils import utils
from utils.column_names.capacity_tracker_columns import (
    CapacityTrackerCareHomeColumns as CTCH,
)
from utils.validation.validation_rules.cleaned_capacity_tracker_care_home_validation_rules import (
    CleanedCapacityTrackerCareHomeValidationRules as Rules,
)
from utils.validation.validation_utils import (
    validate_dataset,
    raise_exception_if_any_checks_failed,
)
from utils.validation.validation_rule_names import RuleNames as RuleName


def main(
    capacity_tracker_care_home_source: str,
    capacity_tracker_care_home_cleaned_source: str,
    report_destination: str,
):
    care_home_df = utils.read_from_parquet(
        capacity_tracker_care_home_source,
    )
    care_home_cleaned_df = utils.read_from_parquet(
        capacity_tracker_care_home_cleaned_source,
    )
    rules = Rules.rules_to_check

    rules[
        RuleName.size_of_dataset
    ] = calculate_expected_size_of_cleaned_capacity_tracker_care_home_dataset(
        care_home_df
    )

    check_result_df = validate_dataset(care_home_cleaned_df, rules)

    utils.write_to_parquet(check_result_df, report_destination, mode="overwrite")

    if isinstance(check_result_df, DataFrame):
        raise_exception_if_any_checks_failed(check_result_df)


def calculate_expected_size_of_cleaned_capacity_tracker_care_home_dataset(
    care_home_df: DataFrame,
) -> int:
    care_home_df = care_home_df.where(
        (
            care_home_df[CTCH.nurses_employed]
            != care_home_df[CTCH.agency_nurses_employed]
        )
        | (
            care_home_df[CTCH.care_workers_employed]
            != care_home_df[CTCH.agency_care_workers_employed]
        )
        | (
            care_home_df[CTCH.non_care_workers_employed]
            != care_home_df[CTCH.agency_non_care_workers_employed]
        )
    )
    expected_size = care_home_df.count()
    return expected_size


if __name__ == "__main__":
    print("Spark job 'validate_cleaned_capacity_tracker_care_home_data' starting...")
    print(f"Job parameters: {sys.argv}")

    (
        capacity_tracker_care_home_source,
        capacity_tracker_care_home_cleaned_source,
        report_destination,
    ) = utils.collect_arguments(
        (
            "--capacity_tracker_care_home_source",
            "Source s3 directory for parquet capacity tracker care home dataset",
        ),
        (
            "--capacity_tracker_care_home_cleaned_source",
            "Source s3 directory for parquet cleaned capacity tracker care home dataset",
        ),
        (
            "--report_destination",
            "Destination s3 directory for validation report parquet",
        ),
    )
    try:
        main(
            capacity_tracker_care_home_source,
            capacity_tracker_care_home_cleaned_source,
            report_destination,
        )
    finally:
        spark = utils.get_spark()
        if spark.sparkContext._gateway:
            spark.sparkContext._gateway.shutdown_callback_server()
        spark.stop()

    print("Spark job 'validate_cleaned_capacity_tracker_care_home_data' complete")
