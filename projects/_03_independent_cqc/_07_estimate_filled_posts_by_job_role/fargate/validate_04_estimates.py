import sys
import polars as pl
import pointblank as pb

from polars_utils import utils
from projects._03_independent_cqc._07_estimate_filled_posts_by_job_role.fargate.utils.validate_utils import (
    create_job_role_estimates_data_validation_columns,
)
from polars_utils.validation import actions as vl
from polars_utils.validation.constants import GLOBAL_ACTIONS, GLOBAL_THRESHOLDS
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns
from utils.column_names.validation_table_columns import Validation as validationColumns
from utils.value_labels.ascwds_worker.ascwds_worker_jobgroup_dictionary import (
    AscwdsWorkerValueLabelsJobGroup as jobGroupDict,
)

ind_cqc_job_role_cols_to_import = [
    IndCqcColumns.id_per_locationid_import_date,
    IndCqcColumns.location_id,
    IndCqcColumns.cqc_location_import_date,
    IndCqcColumns.estimate_filled_posts,
    IndCqcColumns.ascwds_job_role_ratios_merged_source,
    IndCqcColumns.main_job_role_clean_labelled,
    IndCqcColumns.estimate_filled_posts_by_job_role_manager_adjusted,
    IndCqcColumns.estimate_filled_posts_from_all_job_roles,
]

ind_cqc_estimates_cols_to_import = [
    IndCqcColumns.location_id,
    IndCqcColumns.cqc_location_import_date,
]


def main(
    bucket_name: str, source_path: str, compare_path: str, reports_path: str
) -> None:
    """Validates a dataset according to a set of provided rules and produces a
        summary report as well as failure outputs.

    Args:
        bucket_name (str): the bucket (name only) in which to source the dataset
            and output the report to (shoud correspond to workspace / feature
            branch name)
        source_path (str): the source dataset path to be validated
        compare_path (str): the path to the comparison dataset
        reports_path (str): the output path to write reports to
    """
    source_lf = utils.scan_parquet(
        source=f"s3://{bucket_name}/{source_path}",
        selected_columns=ind_cqc_job_role_cols_to_import,
    )
    compare_lf = utils.scan_parquet(
        source=f"s3://{bucket_name}/{compare_path}",
        selected_columns=ind_cqc_estimates_cols_to_import,
    )
    expected_row_count = compare_lf.select(pl.len()).collect().item() * len(
        jobGroupDict.all_roles()
    )
    source_with_validation_columns_lf = (
        create_job_role_estimates_data_validation_columns(source_lf)
    )
    source_df = source_with_validation_columns_lf.collect()

    validation = (
        pb.Validate(
            data=source_df,
            label=f"Validation of {source_path}",
            thresholds=GLOBAL_THRESHOLDS,
            brief=True,
            actions=GLOBAL_ACTIONS,
        )
        # dataset size
        .col_vals_eq(validationColumns.total_job_role_records, expected_row_count)
        # complete columns
        .col_vals_not_null(
            [
                IndCqcColumns.cqc_location_import_date,
            ]
        )
        # index columns
        .rows_distinct(
            [
                IndCqcColumns.cqc_location_import_date,
            ],
        )
        # between (inclusive)
        # .col_vals_between(
        #     IndCqcColumns.national_percentage_care_worker_filled_posts, 0.59, 0.69
        # )
        .col_vals_between(
            IndCqcColumns.national_percentage_direct_care_filled_posts, 0.71, 0.81
        )
        .col_vals_between(
            IndCqcColumns.national_percentage_managers_filled_posts, 0.03, 0.1
        )
        .col_vals_between(
            IndCqcColumns.national_percentage_regulated_professions_filled_posts,
            0.02,
            0.06,
        )
        .col_vals_between(
            IndCqcColumns.national_percentage_other_filled_posts, 0.07, 0.21
        )
        .interrogate()
    )
    vl.write_reports(validation, bucket_name, reports_path)


if __name__ == "__main__":
    print(f"Validation script called with parameters: {sys.argv}")

    args = utils.get_args(
        ("--bucket_name", "S3 bucket for source dataset and validation report"),
        ("--source_path", "The filepath of the dataset to validate"),
        ("--compare_path", "The filepath of the comparison dataset"),
        ("--reports_path", "The filepath to output reports"),
    )
    print(f"Starting validation for {args.source_path}")

    main(args.bucket_name, args.source_path, args.compare_path, args.reports_path)
    print(f"Validation of {args.source_path} complete")
