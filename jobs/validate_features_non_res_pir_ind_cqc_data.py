import os
import sys

# SPARK_VERSION needs to be set before pydeequ is imported
os.environ["SPARK_VERSION"] = "3.3"

from pyspark.sql import DataFrame, functions as F

from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCqc
from utils.column_values.categorical_column_values import CareHome
from utils.validation.validation_rules.features_non_res_pir_ind_cqc_validation_rules import (
    NonResPirIndCqcFeaturesValidationRules as Rules,
)
from utils.validation.validation_rule_names import RuleNames as RuleName
from utils.validation.validation_utils import (
    validate_dataset,
    raise_exception_if_any_checks_failed,
)

cleaned_ind_cqc_columns_to_import = [
    IndCqc.location_id,
    IndCqc.cqc_location_import_date,
    IndCqc.care_home,
    IndCqc.imputed_non_res_pir_people_directly_employed,
]


def main(
    cleaned_ind_cqc_source: str,
    non_res_pir_ind_cqc_features_source: str,
    report_destination: str,
):
    cleaned_ind_cqc_df = utils.read_from_parquet(
        cleaned_ind_cqc_source,
        selected_columns=cleaned_ind_cqc_columns_to_import,
    )
    non_res_pir_ind_cqc_features_df = utils.read_from_parquet(
        non_res_pir_ind_cqc_features_source,
    )
    rules = Rules.rules_to_check

    rules[
        RuleName.size_of_dataset
    ] = calculate_expected_size_of_non_res_pir_ind_cqc_features_dataset(
        cleaned_ind_cqc_df
    )

    check_result_df = validate_dataset(non_res_pir_ind_cqc_features_df, rules)

    utils.write_to_parquet(check_result_df, report_destination, mode="overwrite")

    if isinstance(check_result_df, DataFrame):
        raise_exception_if_any_checks_failed(check_result_df)


def calculate_expected_size_of_non_res_pir_ind_cqc_features_dataset(
    cleaned_ind_cqc_df: DataFrame,
) -> int:
    """
    Calculates the expected size of the non res pir ind cqc features dataset.

    The function calculates the expected size of the non res pir ind cqc features dataset from the previous dataset in the pipeline.

    Args:
        cleaned_ind_cqc_df (DataFrame): A dataframe containing the cleaned ind cqc dataset.

    Returns:
        int: The number of rows expected in the non res pir ind cqc features dataset.
    """
    expected_size = cleaned_ind_cqc_df.filter(
        (F.col(IndCqc.care_home) == CareHome.not_care_home)
        & (F.col(IndCqc.imputed_non_res_pir_people_directly_employed).isNotNull())
    ).count()
    return expected_size


if __name__ == "__main__":
    print("Spark job 'validate_non_res_pir_ind_cqc_features_data' starting...")
    print(f"Job parameters: {sys.argv}")

    (
        cleaned_ind_cqc_source,
        non_res_pir_ind_cqc_features_source,
        report_destination,
    ) = utils.collect_arguments(
        (
            "--cleaned_ind_cqc_source",
            "Source s3 directory for parquet cleaned independent CQC dataset",
        ),
        (
            "--non_res_pir_ind_cqc_features_source",
            "Source s3 directory for parquet non residential PIR independent CQC features dataset",
        ),
        (
            "--report_destination",
            "Destination s3 directory for validation report parquet",
        ),
    )
    try:
        main(
            cleaned_ind_cqc_source,
            non_res_pir_ind_cqc_features_source,
            report_destination,
        )
    finally:
        spark = utils.get_spark()
        if spark.sparkContext._gateway:
            spark.sparkContext._gateway.shutdown_callback_server()
        spark.stop()

    print("Spark job 'validate_non_res_pir_ind_cqc_features_data' complete")
