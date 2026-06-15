import sys
from datetime import date, datetime
from typing import Callable

import pointblank as pb
import polars as pl

from polars_utils import utils
from polars_utils.validation import actions as vl
from polars_utils.validation.constants import GLOBAL_ACTIONS, GLOBAL_THRESHOLDS
from projects._03_independent_cqc._07_estimate_filled_posts_by_job_role.fargate.utils.utils import (
    CategoricalColumnTypes,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns, PartitionKeys
from utils.column_values.categorical_column_values import (
    JobGroupLabels,
    JobRoleFilteringRule,
    MainJobRoleLabels,
)
from utils.column_values.categorical_columns_by_dataset import (
    EstimatedIndCQCFilledPostsByJobRoleCategoricalValues as CatValues,
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
    PartitionKeys.year,
]

COMPARE_COLS = [
    IndCqcColumns.location_id,
    IndCqcColumns.cqc_location_import_date,
]

EXPECTED_SCHEMA = pb.Schema(
    columns={
        IndCqcColumns.id_per_locationid_import_date_job_role: "UInt32",
        IndCqcColumns.location_id: str(CategoricalColumnTypes.LocationCatType),
        IndCqcColumns.cqc_location_import_date: "Date",
        IndCqcColumns.estimate_filled_posts: "Float32",
        IndCqcColumns.primary_service_type: str(
            CategoricalColumnTypes.PrimaryServiceEnumType
        ),
        IndCqcColumns.id_per_locationid_import_date: "UInt32",
        IndCqcColumns.main_job_role_clean_labelled: str(
            CategoricalColumnTypes.JobRoleEnumType
        ),
        IndCqcColumns.ascwds_job_role_counts: "Int16",
        IndCqcColumns.job_role_filtering_rule: str(
            CategoricalColumnTypes.JobRoleFilteringRuleCatType
        ),
        IndCqcColumns.ascwds_job_role_ratios: "Float32",
        IndCqcColumns.imputed_ascwds_job_role_ratios: "Float32",
        IndCqcColumns.imputed_ascwds_job_role_counts: "Float32",
        IndCqcColumns.estimate_filled_posts_size_group: "String",
        IndCqcColumns.ascwds_job_role_rolling_ratio: "Float32",
        IndCqcColumns.ascwds_job_role_ratios_merged: "Float32",
        IndCqcColumns.ascwds_job_role_ratios_merged_source: "String",
        IndCqcColumns.estimate_filled_posts_by_job_role: "Float32",
        IndCqcColumns.estimate_filled_posts_by_job_role_historically_reallocated: "Float32",
        IndCqcColumns.estimate_filled_posts_by_job_role_manager_adjusted: "Float64",
        IndCqcColumns.estimate_filled_posts_from_all_job_roles: "Float64",
        IndCqcColumns.difference_estimate_filled_posts_and_from_all_job_roles: "Float64",
        IndCqcColumns.main_job_group_labelled: str(
            CategoricalColumnTypes.JobGroupEnumType
        ),
        PartitionKeys.year: "Int64",
    }
)

CQC_EARLIEST_IMPORT_DATE = date(2013, 3, 1)

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
                IndCqcColumns.location_id,
                IndCqcColumns.id_per_locationid_import_date_job_role,
                IndCqcColumns.cqc_location_import_date,
                IndCqcColumns.estimate_filled_posts,
                IndCqcColumns.primary_service_type,
                IndCqcColumns.id_per_locationid_import_date,
                IndCqcColumns.main_job_role_clean_labelled,
                IndCqcColumns.job_role_filtering_rule,
                IndCqcColumns.ascwds_job_role_rolling_ratio,
                IndCqcColumns.ascwds_job_role_ratios_merged,
                IndCqcColumns.ascwds_job_role_ratios_merged_source,
                IndCqcColumns.estimate_filled_posts_by_job_role,
                IndCqcColumns.estimate_filled_posts_by_job_role_historically_reallocated,
                IndCqcColumns.estimate_filled_posts_by_job_role_manager_adjusted,
                IndCqcColumns.estimate_filled_posts_from_all_job_roles,
                IndCqcColumns.difference_estimate_filled_posts_and_from_all_job_roles,
                PartitionKeys.year,
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
            IndCqcColumns.ascwds_job_role_ratios_merged_source,
            CatValues.ascwds_job_role_ratios_merged_source_column_values.categorical_values,
        )
        .col_vals_in_set(
            IndCqcColumns.primary_service_type,
            CatValues.primary_service_type_column_values.categorical_values,
        )
        .col_vals_in_set(
            IndCqcColumns.main_job_role_clean_labelled,
            CatValues.main_job_role_labels_column_values.categorical_values,
        )
        .col_vals_in_set(
            IndCqcColumns.main_job_group_labelled,
            CatValues.main_job_group_labels_column_values.categorical_values,
        )
        # distinct values
        .specially(
            vl.is_unique_count_equal(
                IndCqcColumns.ascwds_job_role_ratios_merged_source,
                CatValues.ascwds_job_role_ratios_merged_source_column_values.count_of_categorical_values,
            ),
            brief=f"{IndCqcColumns.ascwds_job_role_ratios_merged_source} should have exactly {CatValues.ascwds_job_role_ratios_merged_source_column_values.count_of_categorical_values} distinct values",
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
        .specially(
            vl.is_unique_count_equal(
                IndCqcColumns.main_job_group_labelled,
                CatValues.main_job_group_labels_column_values.count_of_categorical_values,
            ),
            brief=f"{IndCqcColumns.main_job_group_labelled} should have exactly {CatValues.main_job_group_labels_column_values.count_of_categorical_values} distinct values",
        )
        # numerical
        .col_vals_gt(
            columns=[
                IndCqcColumns.estimate_filled_posts,
                IndCqcColumns.estimate_filled_posts_from_all_job_roles,
            ],
            value=0,
            brief="estimate_filled_posts and estimate_filled_posts_from_all_job_roles should be > 0",
        )
        .col_vals_ge(
            columns=[
                IndCqcColumns.ascwds_job_role_counts,
                IndCqcColumns.imputed_ascwds_job_role_counts,
                IndCqcColumns.ascwds_job_role_ratios_merged,
            ],
            value=0,
            na_pass=True,
            brief="ascwds_job_role_counts should be >= 0 where present",
        )
        .col_vals_ge(
            columns=[
                IndCqcColumns.estimate_filled_posts_by_job_role,
                IndCqcColumns.estimate_filled_posts_by_job_role_historically_reallocated,
                IndCqcColumns.estimate_filled_posts_by_job_role_manager_adjusted,
            ],
            value=0,
            brief="ascwds_job_role_counts should be >= 0 where present",
        )
        .col_vals_between(
            columns=[
                IndCqcColumns.ascwds_job_role_ratios,
                IndCqcColumns.imputed_ascwds_job_role_ratios,
                IndCqcColumns.ascwds_job_role_rolling_ratio,
            ],
            left=0,
            right=1,
            na_pass=True,
            brief="Ratios should be between 0 and 1 where present. Difference between estimate_filled_posts and estimate_filled_posts_from_all_job_roles should be between 0 and 1 where present",
        )
        .col_vals_between(
            columns=IndCqcColumns.difference_estimate_filled_posts_and_from_all_job_roles,
            left=-0.0001,
            right=1,
            na_pass=True,
            brief="Difference between estimate_filled_posts and estimate_filled_posts_from_all_job_roles should be between -0.0001 and 1 where present",
        )
        # Date plausibility
        .col_vals_ge(
            columns=IndCqcColumns.cqc_location_import_date,
            value=CQC_EARLIEST_IMPORT_DATE,
            brief=f"cqc_location_import_date should not be before {CQC_EARLIEST_IMPORT_DATE.strftime('%d/%m/%Y')}",
        )
        .col_vals_between(
            columns=PartitionKeys.year,
            pre=make_convert_col_to_integers_preprocessor(PartitionKeys.year),
            left=2013,
            right=int(datetime.now().year),
            brief="Year should be between 2013 and current year",
        )
        # logical checks
        .col_vals_expr(
            expr=(
                (
                    (
                        (
                            pl.col(IndCqcColumns.job_role_filtering_rule)
                            != pl.lit(JobRoleFilteringRule.populated)
                        )
                        & (pl.col(IndCqcColumns.ascwds_job_role_counts).is_null())
                    )
                    | (
                        (
                            pl.col(IndCqcColumns.job_role_filtering_rule)
                            == pl.lit(JobRoleFilteringRule.populated)
                        )
                        & (pl.col(IndCqcColumns.ascwds_job_role_counts).is_not_null())
                    )
                )
            ),
            brief="ascwds_job_role_counts must be null where job_role_filtering_rule is not populated",
        )
        .specially(
            vl.make_col_has_fewer_nulls_validator(
                IndCqcColumns.imputed_ascwds_job_role_counts,
                IndCqcColumns.ascwds_job_role_counts,
            ),
            brief="imputed_ascwds_job_role_counts should have fewer null values than ascwds_job_role_counts",
        )
        .specially(
            vl.make_col_has_fewer_nulls_validator(
                IndCqcColumns.imputed_ascwds_job_role_ratios,
                IndCqcColumns.ascwds_job_role_ratios,
            ),
            brief="imputed_ascwds_job_role_ratios should have fewer null values than ascwds_job_role_ratios",
        )
        .col_vals_expr(
            expr=(
                (
                    pl.col(IndCqcColumns.ascwds_job_role_counts).is_not_null()
                    & pl.col(IndCqcColumns.ascwds_job_role_ratios).is_not_null()
                )
                | (
                    pl.col(IndCqcColumns.ascwds_job_role_counts).is_null()
                    & pl.col(IndCqcColumns.ascwds_job_role_ratios).is_null()
                )
            ),
            brief="ascwds_job_role_counts and ascwds_job_role_ratios must be populated or not populated on the same rows",
        )
        .col_vals_expr(
            expr=(
                (
                    pl.col(IndCqcColumns.imputed_ascwds_job_role_counts).is_not_null()
                    & pl.col(IndCqcColumns.imputed_ascwds_job_role_ratios).is_not_null()
                )
                | (
                    pl.col(IndCqcColumns.imputed_ascwds_job_role_counts).is_null()
                    & pl.col(IndCqcColumns.imputed_ascwds_job_role_ratios).is_null()
                )
            ),
            brief="imputed_ascwds_job_role_counts and imputed_ascwds_job_role_ratios must be populated or not populated on the same rows",
        )
        .col_vals_expr(
            (
                (pl.col(IndCqcColumns.imputed_ascwds_job_role_ratios).is_null())
                & (
                    pl.col(IndCqcColumns.ascwds_job_role_ratios_merged)
                    == pl.col(IndCqcColumns.ascwds_job_role_rolling_ratio)
                )
            )
            | (
                (pl.col(IndCqcColumns.imputed_ascwds_job_role_ratios).is_not_null())
                & (
                    pl.col(IndCqcColumns.ascwds_job_role_ratios_merged)
                    == pl.col(IndCqcColumns.imputed_ascwds_job_role_ratios)
                )
            ),
            brief="Where imputed_ascwds_job_role_ratios is null, ascwds_job_role_ratios_merged should equal ascwds_job_role_rolling_ratio. Where imputed_ascwds_job_role_ratios is not null, ascwds_job_role_ratios_merged should equal imputed_ascwds_job_role_ratios",
        )
        # estimates between (inclusive)
        .col_vals_expr(
            estimates_percentage_expressions(
                MainJobRoleLabels.care_worker,
                req_pcts[MainJobRoleLabels.care_worker],
                "role",
            ),
            brief="Check percentage of filled posts for care workers",
        )
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
        group per import date across all locations and check if it falls within the specified range.

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


def make_convert_col_to_integers_preprocessor(
    column: str,
) -> Callable[[pl.DataFrame], pl.DataFrame]:
    """
    Creates a preprocessor function that converts a specified column to integers.

    This is used to preprocess the year partition column for validation, allowing
    for checks that require the column to be in a numerical format.

    Args:
        column (str): the name of the column to convert

    Returns:
        Callable[[pl.DataFrame], pl.DataFrame]: the inner function which converts the specified column to integers
    """

    def convert_col_to_integers(df: pl.DataFrame) -> pl.DataFrame:
        return df.with_columns(pl.col(column).cast(pl.Int64))

    return convert_col_to_integers


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
