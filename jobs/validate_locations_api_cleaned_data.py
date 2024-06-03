import os
import sys

os.environ["SPARK_VERSION"] = "3.3"

from pyspark.sql.dataframe import DataFrame

from utils import utils
from utils.column_names.raw_data_files.cqc_location_api_columns import (
    NewCqcLocationApiColumns as CQCL,
)
from utils.column_names.ind_cqc_pipeline_columns import (
    PartitionKeys as Keys,
)
from utils.column_values.categorical_column_values import (
    LocationType,
    RegistrationStatus,
)
from utils.validation.validation_rules.locations_api_cleaned_validation_rules import (
    LocationsAPICleanedValidationRules as Rules,
)
from utils.validation.validation_utils import validate_dataset
from utils.validation.validation_rule_names import RuleNames as RuleName

PartitionKeys = [Keys.year, Keys.month, Keys.day, Keys.import_date]

raw_cqc_locations_columns_to_import = [
    Keys.import_date,
    CQCL.location_id,
    CQCL.type,
    CQCL.registration_status,
]


def main(
    raw_cqc_location_source: str,
    cleaned_cqc_locations_source: str,
    report_destination: str,
):
    raw_location_df = utils.read_from_parquet(
        raw_cqc_location_source,
        selected_columns=raw_cqc_locations_columns_to_import,
    )
    cleaned_cqc_locations_df = utils.read_from_parquet(
        cleaned_cqc_locations_source,
    )
    rules = Rules.rules_to_check

    rules[RuleName.size_of_dataset] = (
        calculate_expected_size_of_cleaned_cqc_locations_dataset(raw_location_df)
    )

    check_result_df = validate_dataset(cleaned_cqc_locations_df, rules)

    utils.write_to_parquet(check_result_df, report_destination, mode="overwrite")


def calculate_expected_size_of_cleaned_cqc_locations_dataset(
    raw_location_df: DataFrame,
) -> int:
    expected_size = raw_location_df.where(
        (raw_location_df[CQCL.type] == LocationType.social_care_identifier)
        & (raw_location_df[CQCL.registration_status] == RegistrationStatus.registered)
    ).count()
    return expected_size


if __name__ == "__main__":
    print("Spark job 'validate_locations_api_cleaned_data' starting...")
    print(f"Job parameters: {sys.argv}")

    (
        raw_cqc_location_source,
        cleaned_cqc_locations_source,
        report_destination,
    ) = utils.collect_arguments(
        (
            "--raw_cqc_location_source",
            "Source s3 directory for parquet locations api dataset",
        ),
        (
            "--cleaned_cqc_locations_source",
            "Source s3 directory for parquet locations api cleaned dataset",
        ),
        (
            "--report_destination",
            "Destination s3 directory for validation report parquet",
        ),
    )
    try:
        main(
            raw_cqc_location_source,
            cleaned_cqc_locations_source,
            report_destination,
        )
    finally:
        spark = utils.get_spark()
        if spark.sparkContext._gateway:
            spark.sparkContext._gateway.shutdown_callback_server()
        spark.stop()

    print("Spark job 'validate_locations_api_cleaned_data' complete")
