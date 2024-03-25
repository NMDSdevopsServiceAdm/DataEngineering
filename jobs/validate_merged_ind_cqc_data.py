import os
import sys

os.environ["SPARK_VERSION"] = "3.3"

from pydeequ.checks import Check, CheckLevel
from pydeequ.verification import VerificationResult, VerificationSuite

from utils import utils
from utils.column_names.cleaned_data_files.cqc_location_cleaned_values import (
    CqcLocationCleanedColumns as CQCLClean,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys

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
    checkResult = (
        VerificationSuite(spark)
        .onData(merged_ind_cqc_df)
        .addCheck(check.isComplete(IndCqcColumns.cqc_sector))
        .addCheck(
            check.hasUniqueness(
                [IndCqcColumns.location_id, IndCqcColumns.cqc_location_import_date],
                lambda x: x == 1,
            )
        )
        .addCheck(check.hasSize(lambda x: x == cqc_location_df_size))
        .run()
    )
    checkResult_df = VerificationResult.checkResultsAsDataFrame(spark, checkResult)
    checkResult_df.show()

    utils.write_to_parquet(checkResult_df, report_destination, mode="overwrite")


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
    main(
        cleaned_cqc_location_source,
        merged_ind_cqc_source,
        report_destination,
    )

    print("Spark job 'validate_merge_ind_cqc_data' complete")
