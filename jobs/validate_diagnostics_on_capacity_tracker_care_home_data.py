import os
import sys

os.environ["SPARK_VERSION"] = "3.3"

from pyspark.sql import DataFrame

from utils import utils
from utils.validation.validation_rules.diagnostics_on_capacity_tracker_care_home_data import (
    DiagnosticsOnCapacityTrackerCareHomeValidationRules as Rules,
)
from utils.validation.validation_utils import (
    validate_dataset,
    raise_exception_if_any_checks_failed,
)


def main(
    capacity_tracker_care_home_diagnostics_source: str,
    report_destination: str,
):
    care_home_diagnostics_df = utils.read_from_parquet(
        capacity_tracker_care_home_diagnostics_source,
    )
    rules = Rules.rules_to_check

    check_result_df = validate_dataset(care_home_diagnostics_df, rules)

    utils.write_to_parquet(check_result_df, report_destination, mode="overwrite")

    if isinstance(check_result_df, DataFrame):
        raise_exception_if_any_checks_failed(check_result_df)


if __name__ == "__main__":
    print(
        "Spark job 'validate_diagnostcs_on_capacity_tracker_care_home_data' starting..."
    )
    print(f"Job parameters: {sys.argv}")

    (
        capacity_tracker_care_home_diagnostics_source,
        report_destination,
    ) = utils.collect_arguments(
        (
            "--capacity_tracker_care_home_diagnostics_source",
            "Source s3 directory for parquet diagnostics on capacity tracker care home dataset",
        ),
        (
            "--report_destination",
            "Destination s3 directory for validation report parquet",
        ),
    )
    try:
        main(
            capacity_tracker_care_home_diagnostics_source,
            report_destination,
        )
    finally:
        spark = utils.get_spark()
        if spark.sparkContext._gateway:
            spark.sparkContext._gateway.shutdown_callback_server()
        spark.stop()

    print(
        "Spark job 'validate_diagnostics_on_capacity_tracker_care_home_data' complete"
    )
