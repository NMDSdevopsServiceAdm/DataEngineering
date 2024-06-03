import os
import sys

os.environ["SPARK_VERSION"] = "3.3"

from pyspark.sql.dataframe import DataFrame

from utils import utils
from utils.column_names.cleaned_data_files.cqc_location_cleaned import (
    CqcLocationCleanedColumns as CQCLClean,
    CqcLocationCleanedValues as CQCLValues,
)
from utils.column_names.ind_cqc_pipeline_columns import (
    PartitionKeys as Keys,
)
from utils.validation.validation_rules.merged_ind_cqc_validation_rules import (
    MergedIndCqcValidationRules as Rules,
)
from utils.validation.validation_utils import validate_dataset
from utils.validation.validation_rule_names import RuleNames as RuleName

PartitionKeys = [Keys.year, Keys.month, Keys.day, Keys.import_date]

cleaned_cqc_locations_columns_to_import = [
    CQCLClean.cqc_location_import_date,
    CQCLClean.location_id,
    CQCLClean.cqc_sector,
]


def main(
    cleaned_cqc_location_source: str,
    merged_ind_cqc_source: str,
    report_destination: str,
):
    cqc_location_df = utils.read_from_parquet(
        cleaned_cqc_location_source,
        selected_columns=cleaned_cqc_locations_columns_to_import,
    )
    merged_ind_cqc_df = utils.read_from_parquet(
        merged_ind_cqc_source,
    )
    rules = Rules.rules_to_check

    rules[RuleName.size_of_dataset] = calculate_expected_size_of_merged_ind_cqc_dataset(
        cqc_location_df
    )

    check_result_df = validate_dataset(merged_ind_cqc_df, rules)

    utils.write_to_parquet(check_result_df, report_destination, mode="overwrite")


def calculate_expected_size_of_merged_ind_cqc_dataset(
    cqc_location_df: DataFrame,
) -> int:
    expected_size = cqc_location_df.where(
        cqc_location_df[CQCLClean.cqc_sector] == CQCLValues.independent
    ).count()
    return expected_size


if __name__ == "__main__":
    print("Spark job 'validate_merge_ind_cqc_data' starting...")
    print(f"Job parameters: {sys.argv}")

    (
        cleaned_cqc_location_source,
        merged_ind_cqc_source,
        report_destination,
    ) = utils.collect_arguments(
        (
            "--cleaned_cqc_location_source",
            "Source s3 directory for parquet CQC locations cleaned dataset",
        ),
        (
            "--merged_ind_cqc_source",
            "Source s3 directory for parquet merged independent CQC dataset",
        ),
        (
            "--report_destination",
            "Destination s3 directory for validation report parquet",
        ),
    )
    try:
        main(
            cleaned_cqc_location_source,
            merged_ind_cqc_source,
            report_destination,
        )
    finally:
        spark = utils.get_spark()
        if spark.sparkContext._gateway:
            spark.sparkContext._gateway.shutdown_callback_server()
        spark.stop()

    print("Spark job 'validate_merge_ind_cqc_data' complete")
