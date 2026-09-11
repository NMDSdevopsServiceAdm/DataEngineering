import sys

import pointblank as pb

from polars_utils import utils
from polars_utils.validation import actions as vl
from polars_utils.validation.constants import GLOBAL_ACTIONS, GLOBAL_THRESHOLDS
from utils.column_names.cleaned_data_files.cqc_pir_cleaned import (
    CqcPIRCleanedColumns as PIRClean,
)
from utils.column_values.categorical_columns_by_dataset import (
    PIRCleanedCategoricalValues as CatValues,
)

COMPLETE_COLUMNS = [
    PIRClean.cqc_pir_import_date,
    PIRClean.location_id,
    PIRClean.pir_people_directly_employed,
    PIRClean.care_home,
]
INDEX_COLUMNS = [
    PIRClean.location_id,
    PIRClean.care_home,
    PIRClean.cqc_pir_import_date,
]


def main(bucket_name: str, source_path: str, reports_path: str) -> None:
    """Validates cleaned CQC PIR data and produces a summary report plus failure outputs.

    Args:
        bucket_name (str): the bucket (name only) in which to source the dataset
            and output the report to (should correspond to workspace / feature
            branch name).
        source_path (str): the source dataset path to be validated.
        reports_path (str): the output path to write reports to.
    """
    cleaned_pir_df = utils.read_parquet(source=f"s3://{bucket_name}/{source_path}")

    care_home_values = CatValues.care_home_column_values

    validation = (
        pb.Validate(
            data=cleaned_pir_df,
            label=f"Validation of {source_path}",
            thresholds=GLOBAL_THRESHOLDS,
            brief=True,
            actions=GLOBAL_ACTIONS,
        )
        # complete columns
        .col_vals_not_null(
            columns=COMPLETE_COLUMNS,
            brief="Key columns should contain no null values",
        )
        # index columns
        .rows_distinct(
            INDEX_COLUMNS,
            brief=f"{INDEX_COLUMNS} together should be unique",
        )
        # min/max value; nulls pass since large single-submission locations
        # are nulled rather than dropped
        .col_vals_between(
            PIRClean.pir_people_directly_employed_cleaned,
            1,
            1500,
            na_pass=True,
            brief=f"{PIRClean.pir_people_directly_employed_cleaned} should be between 1 and 1500",
        )
        # categorical values
        .col_vals_in_set(PIRClean.care_home, care_home_values.categorical_values)
        .specially(
            vl.is_unique_count_equal(
                PIRClean.care_home, care_home_values.count_of_categorical_values
            ),
            brief=f"{PIRClean.care_home} should have exactly {care_home_values.count_of_categorical_values} distinct values",
        )
        .interrogate()
    )
    vl.write_reports(validation, bucket_name, reports_path)


if __name__ == "__main__":
    print(f"Validation script called with parameters: {sys.argv}")

    args = utils.get_args(
        ("--bucket_name", "S3 bucket for source dataset and validation report"),
        ("--source_path", "The filepath of the dataset to validate"),
        ("--reports_path", "The filepath to output reports"),
    )
    print(f"Starting validation for {args.source_path}")

    main(args.bucket_name, args.source_path, args.reports_path)
    print(f"Validation of {args.source_path} complete")
