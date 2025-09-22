import os
import sys

os.environ["SPARK_VERSION"] = "3.5"

from pyspark.sql.dataframe import DataFrame

import utils.cleaning_utils as cUtils
from utils import utils
from utils.column_names.cleaned_data_files.cqc_location_cleaned import (
    CqcLocationCleanedColumns as CQCLClean,
)
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys

from utils.validation.validation_rule_names import RuleNames as RuleName
from utils.validation.validation_rules.merged_coverage_validation_rules import (
    MergedCoverageValidationRules as Rules,
)
from utils.validation.validation_utils import (
    raise_exception_if_any_checks_failed,
    validate_dataset,
)

postcode_dim_import_cols = [CQCLClean.location_id, CQCLClean.postal_code]
gac_dim_import_cols = [CQCLClean.location_id, Keys.import_date]


def main(
    cleaned_cqc_location_source: str,
    gac_dimension_source: str,
    postcode_dimension_source: str,
    merged_coverage_data_source: str,
    report_destination: str,
):
    cqc_location_df = utils.read_from_parquet(
        cleaned_cqc_location_source,
    )
    gac_dimension_df = utils.read_from_parquet(
        gac_dimension_source, selected_columns=gac_dim_import_cols
    )
    cqc_location_df = utils.join_dimension(
        cqc_location_df, gac_dimension_df, primary_key=CQCLClean.location_id
    )

    postcode_dimension_df = utils.read_from_parquet(
        postcode_dimension_source, selected_columns=postcode_dim_import_cols
    )
    cqc_location_df = utils.join_dimension(
        cqc_location_df, postcode_dimension_df, primary_key=CQCLClean.location_id
    )

    merged_coverage_df = utils.read_from_parquet(
        merged_coverage_data_source,
    )
    rules = Rules.rules_to_check

    rules[RuleName.size_of_dataset] = (
        calculate_expected_size_of_merged_coverage_dataset(cqc_location_df)
    )

    check_result_df = validate_dataset(merged_coverage_df, rules)

    utils.write_to_parquet(check_result_df, report_destination, mode="overwrite")

    if isinstance(check_result_df, DataFrame):
        raise_exception_if_any_checks_failed(check_result_df)


def calculate_expected_size_of_merged_coverage_dataset(
    df: DataFrame,
) -> int:
    df = df.dropDuplicates(
        [
            CQCLClean.cqc_location_import_date,
            CQCLClean.name,
            CQCLClean.postal_code,
            CQCLClean.care_home,
        ]
    )
    df = cUtils.reduce_dataset_to_earliest_file_per_month(df)
    expected_size = df.count()
    return expected_size


if __name__ == "__main__":
    print("Spark job 'validate_merge_coverage_data' starting...")
    print(f"Job parameters: {sys.argv}")

    (
        cleaned_cqc_location_source,
        gac_dimension_source,
        postcode_dimension_source,
        merged_coverage_data_source,
        report_destination,
    ) = utils.collect_arguments(
        (
            "--cleaned_cqc_location_source",
            "Source s3 directory for parquet CQC locations cleaned dataset",
        ),
        (
            "--gac_dimension_source",
            "Source S3 directory for parquet GAC services dimension of the cleaned CQC locations dataset",
        ),
        (
            "--postcode_dimension_source",
            "Source S3 directory for parquet postcode matching dimension of the cleaned CQC locations dataset",
        ),
        (
            "--merged_coverage_data_source",
            "Source s3 directory for parquet merged coverage dataset",
        ),
        (
            "--report_destination",
            "Destination s3 directory for validation report parquet",
        ),
    )
    try:
        main(
            cleaned_cqc_location_source,
            gac_dimension_source,
            postcode_dimension_source,
            merged_coverage_data_source,
            report_destination,
        )
    finally:
        spark = utils.get_spark()
        if spark.sparkContext._gateway:
            spark.sparkContext._gateway.shutdown_callback_server()
        spark.stop()

    print("Spark job 'validate_merge_coverage_data' complete")
