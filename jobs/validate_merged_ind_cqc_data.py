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

    cqc_location_df_size = cqc_location_df.count()

    merged_ind_cqc_df = utils.read_from_parquet(
        merged_ind_cqc_source,
    )

    spark = utils.get_spark()

    check = Check(spark, CheckLevel.Warning, "Review Check")
    check_result = (
        VerificationSuite(spark)
        .onData(merged_ind_cqc_df)
        .addCheck(
            check.isComplete(IndCqcColumns.cqc_sector)
            .hasUniqueness(
                [IndCqcColumns.location_id, IndCqcColumns.cqc_location_import_date],
                lambda x: x == 1,
            )
            .hasSize(
                lambda x: x == cqc_location_df_size,
                f"DataFrame row count should be {cqc_location_df_size}",
            )
        )
        .run()
    )
    check_result_df = VerificationResult.checkResultsAsDataFrame(spark, check_result)

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
