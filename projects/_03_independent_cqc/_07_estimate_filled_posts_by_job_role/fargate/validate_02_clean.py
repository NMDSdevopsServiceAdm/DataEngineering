import sys
from datetime import date

import pointblank as pb
import polars as pl

from polars_utils import utils
from polars_utils.validation import actions as vl
from polars_utils.validation.constants import GLOBAL_ACTIONS, GLOBAL_THRESHOLDS
from projects._03_independent_cqc._07_estimate_filled_posts_by_job_role.fargate.utils.utils import (
    CategoricalColumnTypes,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns
from utils.column_values.categorical_columns_by_dataset import (
    EstimatedIndCQCFilledPostsByJobRoleCategoricalValues as CatValues,
)

VALIDATION_COLS_TO_IMPORT = [
    IndCqcColumns.id_per_locationid_import_date,
    IndCqcColumns.location_id,
    IndCqcColumns.cqc_location_import_date,
    IndCqcColumns.id_per_locationid_import_date_job_role,
    IndCqcColumns.estimate_filled_posts,
    IndCqcColumns.primary_service_type,
    IndCqcColumns.main_job_role_clean_labelled,
    IndCqcColumns.ascwds_job_role_counts,
]

IND_CQC_MERGE_COLS_TO_IMPORT = [
    IndCqcColumns.location_id,
    IndCqcColumns.ascwds_job_role_counts,
]

CQC_EARLIEST_IMPORT_DATE = date(2013, 3, 1)

EXPECTED_SCHEMA = pb.Schema(
    columns={
        IndCqcColumns.id_per_locationid_import_date: "UInt32",
        IndCqcColumns.id_per_locationid_import_date_job_role: "UInt32",
        IndCqcColumns.location_id: str(CategoricalColumnTypes.LocationCatType),
        IndCqcColumns.cqc_location_import_date: "Date",
        IndCqcColumns.estimate_filled_posts: "Float32",
        IndCqcColumns.primary_service_type: str(
            CategoricalColumnTypes.PrimaryServiceEnumType
        ),
        IndCqcColumns.main_job_role_clean_labelled: str(
            CategoricalColumnTypes.JobRoleEnumType
        ),
        IndCqcColumns.ascwds_job_role_counts: "Int16",
    }
)


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
        compare_path (str): the filepath of the dataset to compare against
        reports_path (str): the output path to write reports to
    """
    source_df = utils.read_parquet(
        source=f"s3://{bucket_name}/{source_path}",
        selected_columns=VALIDATION_COLS_TO_IMPORT,
    )

    compare_df = utils.read_parquet(
        source=f"s3://{bucket_name}/{compare_path}",
        selected_columns=IND_CQC_MERGE_COLS_TO_IMPORT,
    )

    expected_row_count = compare_df.height

    compare_ascwds_job_role_counts_sum = compare_df[
        IndCqcColumns.ascwds_job_role_counts
    ].sum()

    validation = (
        pb.Validate(
            data=source_df,
            label=f"Validation of {source_path}",
            thresholds=GLOBAL_THRESHOLDS,
            brief=True,
            actions=GLOBAL_ACTIONS,
        )
        # dataset schema
        .col_schema_match(
            schema=EXPECTED_SCHEMA,
            in_order=False,
            brief=f"Dataset schema should match the expected schema",
        )
        # dataset size
        .row_count_match(
            expected_row_count,
            brief=f"Expects {expected_row_count} rows",
        )
        # complete columns
        .col_vals_not_null(
            [
                IndCqcColumns.id_per_locationid_import_date,
                IndCqcColumns.id_per_locationid_import_date_job_role,
                IndCqcColumns.location_id,
                IndCqcColumns.cqc_location_import_date,
                IndCqcColumns.estimate_filled_posts,
                IndCqcColumns.primary_service_type,
                IndCqcColumns.main_job_role_clean_labelled,
            ]
        )
        # index columns
        .rows_distinct(
            [
                IndCqcColumns.id_per_locationid_import_date_job_role,
            ],
        )
        # index columns
        .rows_distinct(
            columns_subset=[
                IndCqcColumns.location_id,
                IndCqcColumns.cqc_location_import_date,
                IndCqcColumns.main_job_role_clean_labelled,
            ],
            brief="Primary key (location_id, cqc_location_import_date, main_job_role_clean_labelled) should be unique",
        )
        .col_vals_expr(
            expr=(
                pl.col(IndCqcColumns.id_per_locationid_import_date)
                .n_unique()
                .over(
                    [
                        IndCqcColumns.location_id,
                        IndCqcColumns.cqc_location_import_date,
                    ]
                )
                == 1
            ),
            brief="id_per_locationid_import_date should be unique per locationid and cqc_location_import_date combination",
        )
        # Cross-dataset aggregate check
        .col_vals_expr(
            expr=(
                pl.col(IndCqcColumns.ascwds_job_role_counts).sum()
                < compare_ascwds_job_role_counts_sum
            ),
            brief=(
                f"Total sum of ascwds_job_role_counts in clean job should be less than the total sum of ascwds_job_role_counts in the merge job dataset"
            ),
        )
        # numerical
        .col_vals_gt(
            columns=IndCqcColumns.estimate_filled_posts,
            value=0,
            na_pass=True,
            brief="estimate_filled_posts and ascwds_filled_posts_dedup_clean should be > 0 where present",
        )
        .col_vals_ge(
            columns=IndCqcColumns.ascwds_job_role_counts,
            value=0,
            na_pass=True,
            brief="ascwds_job_role_counts should be >= 0 where present",
        )
        # categorical
        .col_vals_in_set(
            IndCqcColumns.primary_service_type,
            CatValues.primary_service_type_column_values.categorical_values,
        )
        .col_vals_in_set(
            IndCqcColumns.main_job_role_clean_labelled,
            CatValues.main_job_role_labels_column_values.categorical_values,
        )
        # Date plausibility
        .col_vals_ge(
            columns=IndCqcColumns.cqc_location_import_date,
            value=CQC_EARLIEST_IMPORT_DATE,
            brief=f"cqc_location_import_date should not be before {CQC_EARLIEST_IMPORT_DATE.strftime('%d/%m/%Y')}",
        )
        .interrogate()
    )
    vl.write_reports(validation, bucket_name, reports_path)


if __name__ == "__main__":
    print(f"Validation script called with parameters: {sys.argv}")

    args = utils.get_args(
        ("--bucket_name", "S3 bucket for source dataset and validation report"),
        ("--source_path", "The filepath of the dataset to validate"),
        ("--compare_path", "The filepath of the dataset to compare against"),
        ("--reports_path", "The filepath to output reports"),
    )
    print(f"Starting validation for {args.source_path}")

    main(args.bucket_name, args.source_path, args.compare_path, args.reports_path)
    print(f"Validation of {args.source_path} complete")
