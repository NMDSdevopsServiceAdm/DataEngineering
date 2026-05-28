import gc
import sys
from datetime import date

import pointblank as pb
import polars as pl

from polars_utils import utils
from polars_utils.validation import actions as vl
from polars_utils.validation.constants import GLOBAL_ACTIONS, GLOBAL_THRESHOLDS
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns
from utils.value_labels.ascwds_worker.ascwds_worker_jobgroup_dictionary import (
    AscwdsWorkerValueLabelsJobGroup as jobGroupDict,
)
from utils.column_values.categorical_columns_by_dataset import (
    EstimatedIndCQCFilledPostsCategoricalValues as CatValues,
    ASCWDSWorkerCleanedCategoricalValues as ASCWDSWorkerCatValues,
)

KEY_COLS = [
    IndCqcColumns.id_per_locationid_import_date,
    IndCqcColumns.location_id,
    IndCqcColumns.cqc_location_import_date,
    IndCqcColumns.main_job_role_clean_labelled,
]

CATEGORICAL_COLS = [
    IndCqcColumns.primary_service_type,
    IndCqcColumns.estimate_filled_posts_source,
]

NUMERIC_COLS = [
    IndCqcColumns.estimate_filled_posts,
    IndCqcColumns.ascwds_filled_posts_dedup_clean,
    IndCqcColumns.ascwds_job_role_counts,
]

ind_cqc_estimates_cols_to_import = [
    IndCqcColumns.location_id,
]

CQC_EARLIEST_IMPORT_DATE = date(2013, 3, 1)

KEY_SCHEMA = pb.Schema(
    columns={
        IndCqcColumns.id_per_locationid_import_date: "String",
        IndCqcColumns.location_id: "String",
        IndCqcColumns.cqc_location_import_date: "Date",
        IndCqcColumns.main_job_role_clean_labelled: "String",
    }
)

CATEGORICAL_SCHEMA = pb.Schema(
    columns={
        IndCqcColumns.primary_service_type: "String",
        IndCqcColumns.estimate_filled_posts_source: "String",
        IndCqcColumns.main_job_role_clean_labelled: "String",
    }
)

NUMERIC_SCHEMA = pb.Schema(
    columns={
        IndCqcColumns.estimate_filled_posts: "Float64",
        IndCqcColumns.ascwds_filled_posts_dedup_clean: "Float64",
        IndCqcColumns.ascwds_job_role_counts: "Float64",
    }
)

VALIDATE_KWARGS = dict(
    thresholds=GLOBAL_THRESHOLDS,
    brief=True,
    actions=GLOBAL_ACTIONS,
)


def main(
    bucket_name: str, source_path: str, compare_path: str, reports_path: str
) -> None:
    """Validates a dataset according to a set of provided rules and produces a
        summary report as well as failure outputs.

    Args:
        bucket_name (str): the bucket (name only) in which to source the dataset
            and output the report to (should correspond to workspace / feature
            branch name)
        source_path (str): the source dataset path to be validated
        compare_path (str): the path to the dataset to compare against
        reports_path (str): the output path to write reports to
    """

    # can change name of this. I named it key validation initially because it started with just locartionid and import date validation
    run_distinct_key_validation(source_path, bucket_name, reports_path)
    gc.collect()
    run_other_key_validation(source_path, compare_path, bucket_name, reports_path)
    gc.collect()
    # run_categorical_validation(source_path, bucket_name, reports_path)
    # gc.collect()
    # run_numeric_validation(source_path, bucket_name, reports_path)
    # gc.collect()


def run_distinct_key_validation(source_path, bucket_name, reports_path):
    source_df = utils.read_parquet(
        source=f"s3://{bucket_name}/{source_path}",
        selected_columns=KEY_COLS,
    ).with_columns(pl.col(IndCqcColumns.main_job_role_clean_labelled).cast(pl.String))

    distinct_key_validation = (
        pb.Validate(
            data=source_df,
            label=f"Key validation of {source_path}",
            **VALIDATE_KWARGS,
        )
        .rows_distinct(
            segments=[IndCqcColumns.id_per_locationid_import_date],
            columns_subset=[
                IndCqcColumns.main_job_role_clean_labelled,
            ],
            brief="Primary key (location_id, cqc_location_import_date, main_job_role_clean_labelled) should be unique",
        )
        .interrogate()
    )
    vl.write_reports(
        distinct_key_validation, bucket_name, f"{reports_path}distinct_key/"
    )
    del source_df, distinct_key_validation


def run_other_key_validation(source_path, compare_path, bucket_name, reports_path):
    source_df = utils.read_parquet(
        source=f"s3://{bucket_name}/{source_path}",
        selected_columns=KEY_COLS,
    ).with_columns(pl.col(IndCqcColumns.main_job_role_clean_labelled).cast(pl.String))
    compare_df = utils.read_parquet(
        source=f"s3://{bucket_name}/{compare_path}",
        selected_columns=ind_cqc_estimates_cols_to_import,
    )
    expected_row_count = compare_df.height * len(jobGroupDict.all_roles())

    other_key_validation = (
        pb.Validate(
            data=source_df,
            label=f"Key validation of {source_path}",
            **VALIDATE_KWARGS,
        )
        .col_schema_match(KEY_SCHEMA)
        .row_count_match(
            expected_row_count,
            brief=f"Expects {expected_row_count} rows",
        )
        .col_vals_not_null(
            columns=KEY_COLS,
            brief="Key columns should contain no null values",
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
        # had to do it this way because ASCWDSWorkerCatValues.main_job_role_labels_column_values.categorical_values has extra values like technician which we dont have in data but is used elsewhere in pipeline so cant just delete it
        .col_vals_expr(
            expr=pl.col(IndCqcColumns.main_job_role_clean_labelled)
            .cast(pl.String)
            .is_in(
                ASCWDSWorkerCatValues.main_job_role_labels_column_values.categorical_values
            ),
            brief="main_job_role_clean_labelled should only contain recognised job role categories",
        )
        .interrogate()
    )
    vl.write_reports(other_key_validation, bucket_name, f"{reports_path}other_key/")
    del source_df, compare_df, other_key_validation


def run_categorical_validation(source_path, bucket_name, reports_path):
    categorical_df = utils.read_parquet(
        source=f"s3://{bucket_name}/{source_path}",
        selected_columns=CATEGORICAL_COLS,
    )

    categorical_validation = (
        pb.Validate(
            data=categorical_df,
            label=f"Categorical validation of {source_path}",
            **VALIDATE_KWARGS,
        )
        .col_schema_match(CATEGORICAL_SCHEMA)
        .col_vals_not_null(
            columns=CATEGORICAL_COLS,
            brief="Categorical columns should contain no null values",
        )
        .col_vals_in_set(
            IndCqcColumns.estimate_filled_posts_source,
            CatValues.estimate_filled_posts_source_column_values.categorical_values,
        )
        .col_vals_in_set(
            IndCqcColumns.primary_service_type,
            CatValues.primary_service_type_column_values.categorical_values,
        )
        .specially(
            vl.is_unique_count_equal(
                IndCqcColumns.estimate_filled_posts_source,
                CatValues.estimate_filled_posts_source_column_values.count_of_categorical_values,
            ),
            brief=f"{IndCqcColumns.estimate_filled_posts_source} should have exactly {CatValues.estimate_filled_posts_source_column_values.count_of_categorical_values} distinct values",
        )
        .specially(
            vl.is_unique_count_equal(
                IndCqcColumns.primary_service_type,
                CatValues.primary_service_type_column_values.count_of_categorical_values,
            ),
            brief=f"{IndCqcColumns.primary_service_type} should have exactly {CatValues.primary_service_type_column_values.count_of_categorical_values} distinct values",
        )
        .interrogate()
    )
    vl.write_reports(categorical_validation, bucket_name, f"{reports_path}categorical/")
    del categorical_df, categorical_validation


def run_numeric_validation(source_path, bucket_name, reports_path):
    numeric_df = utils.read_parquet(
        source=f"s3://{bucket_name}/{source_path}",
        selected_columns=NUMERIC_COLS,
    )

    numeric_validation = (
        pb.Validate(
            data=numeric_df,
            label=f"Numeric validation of {source_path}",
            **VALIDATE_KWARGS,
        )
        .col_schema_match(NUMERIC_SCHEMA)
        .col_vals_not_null(
            columns=[IndCqcColumns.estimate_filled_posts],
            brief="estimate_filled_posts should contain no null values",
        )
        .col_vals_gt(
            columns=[
                IndCqcColumns.estimate_filled_posts,
                IndCqcColumns.ascwds_filled_posts_dedup_clean,
            ],
            value=0,
            na_pass=True,
            brief="estimate_filled_posts and ascwds_filled_posts_dedup_clean should be > 0 where present",
        )
        .col_vals_ge(
            columns=[IndCqcColumns.ascwds_job_role_counts],
            value=0,
            na_pass=True,
            brief="ascwds_job_role_counts should be >= 0 where present",
        )
        # had to do it this way as multi conditional checks are not possible directly in Pointblank
        # Currently failing
        .col_vals_expr(
            expr=(
                pl.col(IndCqcColumns.ascwds_job_role_counts).is_null()
                | pl.col(IndCqcColumns.estimate_filled_posts).is_null()
                | (
                    pl.col(IndCqcColumns.ascwds_job_role_counts)
                    <= pl.col(IndCqcColumns.estimate_filled_posts)
                )
            ),
            brief="ascwds_job_role_counts <= estimate_filled_posts where both are present",
        )
        .interrogate()
    )
    vl.write_reports(numeric_validation, bucket_name, f"{reports_path}numeric/")
    del numeric_df, numeric_validation


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
