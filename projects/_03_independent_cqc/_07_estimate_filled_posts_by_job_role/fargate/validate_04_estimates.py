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

ind_cqc_job_role_cols_to_import = [
    IndCqcColumns.id_per_locationid_import_date,
    IndCqcColumns.location_id,
    IndCqcColumns.cqc_location_import_date,
    IndCqcColumns.estimate_filled_posts,
    IndCqcColumns.ascwds_job_role_ratios_merged_source,
    IndCqcColumns.main_job_role_clean_labelled,
    IndCqcColumns.estimate_filled_posts_by_job_role_manager_adjusted,
    IndCqcColumns.estimate_filled_posts_from_all_job_roles,
    IndCqcColumns.main_job_group_labelled,
]

ind_cqc_estimates_cols_to_import = [
    IndCqcColumns.location_id,
    IndCqcColumns.cqc_location_import_date,
]

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
        selected_columns=ind_cqc_job_role_cols_to_import,
    )
    compare_df = utils.read_parquet(
        source=f"s3://{bucket_name}/{compare_path}",
        selected_columns=ind_cqc_estimates_cols_to_import,
    )
    # expected_row_count = compare_df.height

    validation = (
        pb.Validate(
            data=source_df,
            label=f"Validation of {source_path}",
            thresholds=GLOBAL_THRESHOLDS,
            brief=True,
            actions=GLOBAL_ACTIONS,
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
