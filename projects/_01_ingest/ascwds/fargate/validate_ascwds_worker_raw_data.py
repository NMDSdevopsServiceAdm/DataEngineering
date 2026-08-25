import sys

import pointblank as pb

from polars_utils import utils
from polars_utils.validation import actions as vl
from polars_utils.validation.constants import GLOBAL_ACTIONS, GLOBAL_THRESHOLDS
from utils.column_names.raw_data_files.ascwds_worker_columns import (
    AscwdsWorkerColumns as AWK,
)
from utils.column_values.categorical_columns_by_dataset import (
    ASCWDSWorkerRawCategoricalValues as CatValues,
)


def main(bucket_name: str, source_path: str, reports_path: str) -> None:
    """Validates raw ASCWDS worker data and produces a summary report plus failure outputs.

    The main job role column is still allowed to contain the sentinel unknown
    value "-1" here, even though new ingestion now hard-fails on it, because
    historical raw data already contains legitimate "-1" rows.

    Args:
        bucket_name (str): the bucket (name only) in which to source the dataset
            and output the report to (should correspond to workspace / feature
            branch name).
        source_path (str): the source dataset path to be validated.
        reports_path (str): the output path to write reports to.
    """
    source_df = utils.read_parquet(source=f"s3://{bucket_name}/{source_path}")

    known_values = CatValues.main_job_role_id_column_values.categorical_values
    count_of_known_values = (
        CatValues.main_job_role_id_column_values.count_of_categorical_values
    )

    validation = (
        pb.Validate(
            data=source_df,
            label=f"Validation of {source_path}",
            thresholds=GLOBAL_THRESHOLDS,
            brief=True,
            actions=GLOBAL_ACTIONS,
        )
        # complete columns
        .col_vals_not_null(
            columns=[
                AWK.establishment_id,
                AWK.worker_id,
                AWK.main_job_role_id,
                AWK.import_date,
            ],
            brief="Key columns should contain no null values",
        )
        # categorical values
        .col_vals_in_set(
            AWK.main_job_role_id,
            [*known_values, "-1"],
            # "-1" (unknown) is allowed here even though ingest now hard-fails on
            # it in new files, since historical raw data already contains
            # legitimate "-1" rows.
            brief=f"{AWK.main_job_role_id} should be a known job role id, or -1 (unknown)",
        )
        # distinct values
        .specially(
            vl.is_unique_count_equal(AWK.main_job_role_id, count_of_known_values + 1),
            brief=f"{AWK.main_job_role_id} should have exactly {count_of_known_values + 1} distinct values",
        ).interrogate()
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
