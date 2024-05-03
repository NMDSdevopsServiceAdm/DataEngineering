import os
import sys

os.environ["SPARK_VERSION"] = "3.3"

from pydeequ.checks import Check, CheckLevel
from pydeequ.verification import VerificationResult, VerificationSuite
from pyspark.sql.dataframe import DataFrame

from utils import utils
from utils.column_names.cleaned_data_files.cqc_location_cleaned_values import (
    CqcLocationCleanedColumns as CQCLClean,
)
from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns,
    PartitionKeys as Keys,
)
from utils.validation.validation_rules.merged_ind_cqc_validation_rules import (
    MergedIndCqcValidationRules as Rules,
)
import utils.validation.validation_utils as Vutils

PartitionKeys = [Keys.year, Keys.month, Keys.day, Keys.import_date]

cleaned_cqc_locations_columns_to_import = [
    CQCLClean.cqc_location_import_date,
    CQCLClean.location_id,
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
    spark = utils.get_spark()

    expected_size = cqc_location_df.count()

    check_dataset_size = Vutils.create_check_of_size_of_dataset(expected_size)
    check_index_columns_are_unique = (
        Vutils.create_check_of_uniqueness_of_two_index_columns(Rules.index_columns)
    )
    check_column_completeness = Vutils.create_check_for_column_completeness(
        Rules.complete_columns
    )

    check_result = (
        VerificationSuite(spark)
        .onData(merged_ind_cqc_df)
        .addCheck(check_dataset_size)
        .addCheck(check_index_columns_are_unique)
        .addCheck(check_column_completeness)
        .run()
    )
    check_result_df = VerificationResult.checkResultsAsDataFrame(spark, check_result)
    check_result_df.show()

    utils.write_to_parquet(check_result_df, report_destination, mode="overwrite")

    parse_data_quality_errors(check_result_df)


def parse_data_quality_errors(check_results: DataFrame):
    failures_df = check_results.where(check_results["constraint_status"] == "Failure")

    failures_count = failures_df.count()
    if failures_count == 0:
        return

    print(f"{failures_count} data quaility failures detected, printing errors")

    for failure in failures_df.collect():
        print(failure.asDict()["constraint_message"])


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
