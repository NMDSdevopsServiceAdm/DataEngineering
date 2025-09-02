import sys

from pyspark.sql.dataframe import DataFrame

from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import (
    PartitionKeys as Keys,
)
from utils.validation.validation_rule_names import RuleNames as RuleName
from utils.validation.validation_rules.estimated_ind_cqc_filled_posts_validation_rules import (
    EstimatedIndCqcFilledPostsValidationRules as Rules,
)
from utils.validation.validation_utils import (
    raise_exception_if_any_checks_failed,
    validate_dataset,
)

PartitionKeys = [Keys.year, Keys.month, Keys.day, Keys.import_date]


def main(
    cleaned_ind_cqc_source: str,
    estimated_ind_cqc_filled_posts_source: str,
    report_destination: str,
):
    cleaned_ind_cqc_df = utils.read_from_parquet(
        cleaned_ind_cqc_source,
    )
    estimated_ind_cqc_filled_posts_df = utils.read_from_parquet(
        estimated_ind_cqc_filled_posts_source,
    )
    rules = Rules.rules_to_check

    rules[RuleName.size_of_dataset] = (
        calculate_expected_size_of_estimated_ind_cqc_filled_posts_dataset(
            cleaned_ind_cqc_df
        )
    )

    check_result_df = validate_dataset(estimated_ind_cqc_filled_posts_df, rules)

    utils.write_to_parquet(check_result_df, report_destination, mode="overwrite")

    if isinstance(check_result_df, DataFrame):
        raise_exception_if_any_checks_failed(check_result_df)


def calculate_expected_size_of_estimated_ind_cqc_filled_posts_dataset(
    cleaned_ind_cqc_df: DataFrame,
) -> int:
    expected_size = cleaned_ind_cqc_df.count()
    return expected_size


if __name__ == "__main__":
    print("Spark job 'validate_estimated_ind_cqc_filled_posts_source_data' starting...")
    print(f"Job parameters: {sys.argv}")

    (
        cleaned_ind_cqc_source,
        estimated_ind_cqc_filled_posts_source,
        report_destination,
    ) = utils.collect_arguments(
        (
            "--cleaned_ind_cqc_source",
            "Source s3 directory for parquet cleaned independent CQC dataset",
        ),
        (
            "--estimated_ind_cqc_filled_posts_source",
            "Source s3 directory for parquet estimated independent CQC filled posts dataset",
        ),
        (
            "--report_destination",
            "Destination s3 directory for validation report parquet",
        ),
    )
    try:
        main(
            cleaned_ind_cqc_source,
            estimated_ind_cqc_filled_posts_source,
            report_destination,
        )
    finally:
        spark = utils.get_spark()
        if spark.sparkContext._gateway:
            spark.sparkContext._gateway.shutdown_callback_server()
        spark.stop()

    print("Spark job 'validate_estimated_ind_cqc_filled_posts_source_data' complete")
