import sys
import polars as pl
import pointblank as pb

from polars_utils import utils
from polars_utils.validation import actions as vl
from polars_utils.validation.constants import GLOBAL_ACTIONS, GLOBAL_THRESHOLDS
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns
from utils.column_values.categorical_column_values import (
    JobGroupLabels,
    MainJobRoleLabels,
)

VALIDATION_COLS = [
    IndCqcColumns.id_per_locationid_import_date_job_role,
    IndCqcColumns.location_id,
    IndCqcColumns.cqc_location_import_date,
    IndCqcColumns.estimate_filled_posts,
    IndCqcColumns.primary_service_type,
    IndCqcColumns.id_per_locationid_import_date,
    IndCqcColumns.main_job_role_clean_labelled,
    IndCqcColumns.ascwds_job_role_counts,
    IndCqcColumns.job_role_filtering_rule,
    IndCqcColumns.ascwds_job_role_ratios,
    IndCqcColumns.imputed_ascwds_job_role_ratios,
    IndCqcColumns.imputed_ascwds_job_role_counts,
    IndCqcColumns.estimate_filled_posts_size_group,
    IndCqcColumns.ascwds_job_role_rolling_ratio,
    IndCqcColumns.ascwds_job_role_ratios_merged,
    IndCqcColumns.ascwds_job_role_ratios_merged_source,
    IndCqcColumns.estimate_filled_posts_by_job_role,
    IndCqcColumns.estimate_filled_posts_by_job_role_historically_reallocated,
    IndCqcColumns.estimate_filled_posts_by_job_role_manager_adjusted,
    IndCqcColumns.estimate_filled_posts_from_all_job_roles,
    IndCqcColumns.difference_estimate_filled_posts_and_from_all_job_roles,
    IndCqcColumns.main_job_group_labelled,
]

COMPARE_COLS = [
    IndCqcColumns.location_id,
    IndCqcColumns.cqc_location_import_date,
]

EXPECTED_SCHEMA = {
    IndCqcColumns.id_per_locationid_import_date_job_role: pl.Utf8,
    IndCqcColumns.location_id: pl.Utf8,
    IndCqcColumns.cqc_location_import_date: pl.Date,
    IndCqcColumns.estimate_filled_posts: pl.Int64,
    IndCqcColumns.primary_service_type: pl.Utf8,
    IndCqcColumns.id_per_locationid_import_date: pl.Utf8,
    IndCqcColumns.main_job_role_clean_labelled: pl.Utf8,
    IndCqcColumns.ascwds_job_role_counts: pl.Int16,
    IndCqcColumns.job_role_filtering_rule: pl.Utf8,
    IndCqcColumns.ascwds_job_role_ratios: pl.Float32,
    IndCqcColumns.imputed_ascwds_job_role_ratios: pl.Float32,
    IndCqcColumns.imputed_ascwds_job_role_counts: pl.Float32,
    IndCqcColumns.estimate_filled_posts_size_group: pl.Utf8,
    IndCqcColumns.ascwds_job_role_rolling_ratio: pl.Float32,
    IndCqcColumns.ascwds_job_role_ratios_merged: pl.Float32,
    IndCqcColumns.ascwds_job_role_ratios_merged_source: pl.Utf8,
    IndCqcColumns.estimate_filled_posts_by_job_role: pl.Float32,
    IndCqcColumns.estimate_filled_posts_by_job_role_historically_reallocated: pl.Float32,
    IndCqcColumns.estimate_filled_posts_by_job_role_manager_adjusted: pl.Int64,
    IndCqcColumns.estimate_filled_posts_from_all_job_roles: pl.Int64,
    IndCqcColumns.difference_estimate_filled_posts_and_from_all_job_roles: pl.Int64,
    IndCqcColumns.main_job_group_labelled: pl.Utf8,
}

req_pcts = {
    MainJobRoleLabels.care_worker: (0.59, 0.69),
    JobGroupLabels.direct_care: (0.71, 0.81),
    JobGroupLabels.managers: (0.03, 0.1),
    JobGroupLabels.regulated_professions: (0.02, 0.06),
    JobGroupLabels.other: (0.07, 0.21),
}


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
    source_df = utils.read_parquet(
        source=f"s3://{bucket_name}/{source_path}",
        selected_columns=VALIDATION_COLS,
    )
    compare_df = utils.read_parquet(
        source=f"s3://{bucket_name}/{compare_path}",
        selected_columns=COMPARE_COLS,
    )
    expected_row_count = compare_df.height

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
            schema=EXPECTED_SCHEMA, brief="Dataset should match the expected schema"
        )
        # dataset size
        .row_count_match(
            expected_row_count,
            brief=f"Expects {expected_row_count} rows",
        )
        # complete columns
        .col_vals_not_null(
            columns=[
                IndCqcColumns.id_per_locationid_import_date,
                IndCqcColumns.location_id,
                IndCqcColumns.cqc_location_import_date,
                IndCqcColumns.primary_service_type,
                IndCqcColumns.main_job_role_clean_labelled,
                IndCqcColumns.estimate_filled_posts_source,
                IndCqcColumns.estimate_filled_posts,
            ],
            brief="Key columns should contain no null values",
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
        .rows_distinct(
            columns_subset=[
                IndCqcColumns.id_per_locationid_import_date_job_role,
            ],
            brief="ID key should be unique",
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
        # categorical
        .col_vals_in_set(
            IndCqcColumns.estimate_filled_posts_source,
            CatValues.estimate_filled_posts_source_column_values.categorical_values,
        )
        .col_vals_in_set(
            IndCqcColumns.primary_service_type,
            CatValues.primary_service_type_column_values.categorical_values,
        )
        .col_vals_in_set(
            IndCqcColumns.main_job_role_clean_labelled,
            CatValues.main_job_role_labels_column_values.categorical_values,
        )
        # distinct values
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
        .specially(
            vl.is_unique_count_equal(
                IndCqcColumns.main_job_role_clean_labelled,
                CatValues.main_job_role_labels_column_values.count_of_categorical_values,
            ),
            brief=f"{IndCqcColumns.main_job_role_clean_labelled} should have exactly {CatValues.main_job_role_labels_column_values.count_of_categorical_values} distinct values",
        )
        # numerical
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
            columns=IndCqcColumns.ascwds_job_role_counts,
            value=0,
            na_pass=True,
            brief="ascwds_job_role_counts should be >= 0 where present",
        )
        # Date plausibility
        .col_vals_ge(
            columns=IndCqcColumns.cqc_location_import_date,
            value=CQC_EARLIEST_IMPORT_DATE,
            brief=f"cqc_location_import_date should not be before {CQC_EARLIEST_IMPORT_DATE.strftime('%d/%m/%Y')}",
        )
        # estimates between (inclusive)
        # .col_vals_expr(
        #     estimates_percentage_expressions(
        #         MainJobRoleLabels.care_worker,
        #         req_pcts[MainJobRoleLabels.care_worker],
        #         "role",
        #     ),
        #     brief="Check percentage of filled posts for care workers",
        # )
        .col_vals_expr(
            estimates_percentage_expressions(
                JobGroupLabels.direct_care,
                req_pcts[JobGroupLabels.direct_care],
                "group",
            ),
            brief="Check percentage of filled posts for direct care",
        )
        .col_vals_expr(
            estimates_percentage_expressions(
                JobGroupLabels.managers, req_pcts[JobGroupLabels.managers], "group"
            ),
            brief="Check percentage of filled posts for managers",
        )
        .col_vals_expr(
            estimates_percentage_expressions(
                JobGroupLabels.regulated_professions,
                req_pcts[JobGroupLabels.regulated_professions],
                "group",
            ),
            brief="Check percentage of filled posts for regulated professions",
        )
        .col_vals_expr(
            estimates_percentage_expressions(
                JobGroupLabels.other, req_pcts[JobGroupLabels.other], "group"
            ),
            brief="Check percentage of filled posts for other",
        )
        .interrogate()
    )
    vl.write_reports(validation, bucket_name, reports_path)


def estimates_percentage_expressions(
    name: str, pcts: tuple[float], role_or_group: str
) -> pl.Expr:
    """
    Constructs an expression to calculate the percentage of filled posts for a given job role or
        group and check if it falls within the specified range.

    Args:
        name (str): the name of the job role or group to calculate the percentage for
        pcts (tuple[float]): the lower and upper bounds for the acceptable percentage range
        role_or_group (str): specifies whether to calculate for a job role or group

    Returns:
        pl.Expr: the expression for validating the percentage

    Raises:
        ValueError: if role_or_group is not 'role' or 'group', or if pcts is not a list of two numbers
    """
    if role_or_group not in ["role", "group"]:
        raise ValueError("role_or_group must be either 'role' or 'group'")
    if len(pcts) != 2 or not all(isinstance(pct, (int, float)) for pct in pcts):
        raise ValueError(
            "pcts must be a tuple of two values: (lower_bound, upper_bound)"
        )
    if role_or_group == "role":
        expr = (
            pl.when(pl.col(IndCqcColumns.main_job_role_clean_labelled) == name)
            .then(
                pl.col(IndCqcColumns.estimate_filled_posts_by_job_role_manager_adjusted)
            )
            .otherwise(0)
            .sum()
            / pl.col(
                IndCqcColumns.estimate_filled_posts_by_job_role_manager_adjusted
            ).sum()
        )

    elif role_or_group == "group":
        expr = (
            pl.when(pl.col(IndCqcColumns.main_job_group_labelled) == name)
            .then(
                pl.col(IndCqcColumns.estimate_filled_posts_by_job_role_manager_adjusted)
            )
            .otherwise(0)
            .sum()
            / pl.col(
                IndCqcColumns.estimate_filled_posts_by_job_role_manager_adjusted
            ).sum()
        )
    return ((expr >= pcts[0]) & (expr <= pcts[1])).over(
        pl.col(IndCqcColumns.cqc_location_import_date)
    )


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
