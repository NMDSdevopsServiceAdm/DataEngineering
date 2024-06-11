import os
import sys

os.environ["SPARK_VERSION"] = "3.3"

from utils import utils
from utils.column_names.raw_data_files.cqc_location_api_columns import (
    NewCqcLocationApiColumns as CQCL,
)
from utils.validation.validation_rules.locations_api_raw_validation_rules import (
    LocationsAPIRawValidationRules as Rules,
)
from utils.validation.validation_utils import (
    validate_dataset,
    add_column_with_length_of_string,
)
from utils.validation.validation_rule_names import RuleNames as RuleName


def main(
    raw_cqc_location_source: str,
    report_destination: str,
):
    raw_location_df = utils.read_from_parquet(
        raw_cqc_location_source,
    )
    rules = Rules.rules_to_check

    raw_location_df = add_column_with_length_of_string(
        raw_location_df, [CQCL.location_id, CQCL.provider_id]
    )

    check_result_df = validate_dataset(raw_location_df, rules)

    utils.write_to_parquet(check_result_df, report_destination, mode="overwrite")


if __name__ == "__main__":
    print("Spark job 'validate_locations_api_raw_data' starting...")
    print(f"Job parameters: {sys.argv}")

    (
        raw_cqc_location_source,
        report_destination,
    ) = utils.collect_arguments(
        (
            "--raw_cqc_location_source",
            "Source s3 directory for parquet locations api dataset",
        ),
        (
            "--report_destination",
            "Destination s3 directory for validation report parquet",
        ),
    )
    try:
        main(
            raw_cqc_location_source,
            report_destination,
        )
    finally:
        spark = utils.get_spark()
        if spark.sparkContext._gateway:
            spark.sparkContext._gateway.shutdown_callback_server()
        spark.stop()

    print("Spark job 'validate_locations_api_raw_data' complete")
