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
from utils.column_values.categorical_column_values import JobRoleFilteringRule
from utils.column_values.categorical_columns_by_dataset import (
    EstimatedIndCQCFilledPostsByJobRoleCategoricalValues as CatValues,
)

VALIDATION_COLS_TO_IMPORT = [
    IndCqcColumns.id_per_locationid_import_date,
    IndCqcColumns.id_per_locationid_import_date_job_role,
    IndCqcColumns.location_id,
    IndCqcColumns.cqc_location_import_date,
    IndCqcColumns.main_job_role_clean_labelled,
    IndCqcColumns.primary_service_type,
    IndCqcColumns.estimate_filled_posts,
    IndCqcColumns.ascwds_job_role_counts,
    IndCqcColumns.job_role_filtering_rule,
    IndCqcColumns.ascwds_job_role_ratios,
    IndCqcColumns.imputed_ascwds_job_role_ratios,
    IndCqcColumns.imputed_ascwds_job_role_counts,
    IndCqcColumns.estimate_filled_posts_size_group,
    IndCqcColumns.ascwds_job_role_rolling_ratio,
]

IND_CQC_ESTIMATES_COLS_TO_IMPORT = [
    IndCqcColumns.location_id,
]

CQC_EARLIEST_IMPORT_DATE = date(2013, 3, 1)

EXPECTED_SCHEMA = pb.Schema(
    columns={
        IndCqcColumns.id_per_locationid_import_date: "UInt32",
        IndCqcColumns.id_per_locationid_import_date_job_role: "UInt32",
        IndCqcColumns.location_id: str(CategoricalColumnTypes.LocationCatType),
        IndCqcColumns.cqc_location_import_date: "Date",
        IndCqcColumns.main_job_role_clean_labelled: str(
            CategoricalColumnTypes.JobRoleEnumType
        ),
        IndCqcColumns.primary_service_type: str(
            CategoricalColumnTypes.PrimaryServiceEnumType
        ),
        IndCqcColumns.estimate_filled_posts: "Float32",
        IndCqcColumns.ascwds_job_role_counts: "Int16",
        IndCqcColumns.job_role_filtering_rule: str(
            CategoricalColumnTypes.JobRoleFilteringRuleCatType
        ),
        IndCqcColumns.ascwds_job_role_ratios: "Float32",
        IndCqcColumns.imputed_ascwds_job_role_ratios: "Float32",
        IndCqcColumns.imputed_ascwds_job_role_counts: "Float32",
        IndCqcColumns.estimate_filled_posts_size_group: "String",
        IndCqcColumns.ascwds_job_role_rolling_ratio: "Float32",
    }
)


def count_nulls(df: pl.DataFrame) -> pl.DataFrame:
    """
    Helper function to count null values in specific columns.
    """

    cols_to_count_nulls = [
        IndCqcColumns.imputed_ascwds_job_role_counts,
        IndCqcColumns.ascwds_job_role_counts,
        IndCqcColumns.imputed_ascwds_job_role_ratios,
        IndCqcColumns.ascwds_job_role_ratios,
    ]

    return df.select(
        [pl.col(column).is_null().sum().alias(column) for column in cols_to_count_nulls]
    )


def uniqueness_id_per_locationid_import_date(df: pl.DataFrame) -> pl.DataFrame:
    """
    Helper function to check uniqueness of id_per_locationid_import_date within
    location_id and cqc_location_import_date combinations.
    """

    return df.group_by(
        [IndCqcColumns.location_id, IndCqcColumns.cqc_location_import_date]
    ).agg(
        pl.col(IndCqcColumns.id_per_locationid_import_date)
        .n_unique()
        .alias("uniqueness_id_per_locationid_import_date")
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
    source_df = utils.read_parquet(
        source=f"s3://{bucket_name}/{source_path}",
        selected_columns=VALIDATION_COLS_TO_IMPORT,
    )
    compare_df = utils.read_parquet(
        source=f"s3://{bucket_name}/{compare_path}",
        selected_columns=IND_CQC_ESTIMATES_COLS_TO_IMPORT,
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
                IndCqcColumns.id_per_locationid_import_date_job_role,
                IndCqcColumns.location_id,
                IndCqcColumns.cqc_location_import_date,
                IndCqcColumns.primary_service_type,
                IndCqcColumns.main_job_role_clean_labelled,
                IndCqcColumns.estimate_filled_posts,
                IndCqcColumns.job_role_filtering_rule,
                IndCqcColumns.estimate_filled_posts_size_group,
                IndCqcColumns.ascwds_job_role_rolling_ratio,
            ],
            brief="Key columns should contain no null values",
        )
        # index columns
        # .rows_distinct(
        #     columns_subset=IndCqcColumns.id_per_locationid_import_date_job_role,
        #     brief="id_per_locationid_import_date_job_role should be unique",
        # )
        .rows_distinct(
            columns_subset=[
                IndCqcColumns.location_id,
                IndCqcColumns.cqc_location_import_date,
                IndCqcColumns.main_job_role_clean_labelled,
            ],
            brief="Primary key (location_id, cqc_location_import_date, main_job_role_clean_labelled) should be unique",
        )
        .col_vals_expr(
            pre=uniqueness_id_per_locationid_import_date,
            expr=pl.col("uniqueness_id_per_locationid_import_date") == 1,
            brief="id_per_locationid_import_date should be unique per locationid and cqc_location_import_date combination",
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
        # distinct values
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
        .col_vals_expr(
            expr=(
                (
                    (
                        (
                            pl.col(IndCqcColumns.job_role_filtering_rule)
                            != pl.lit(JobRoleFilteringRule.populated)
                        )
                        & pl.col(IndCqcColumns.ascwds_job_role_counts).is_null()
                    )
                    | (
                        (
                            pl.col(IndCqcColumns.job_role_filtering_rule)
                            == pl.lit(JobRoleFilteringRule.populated)
                        )
                        & pl.col(IndCqcColumns.ascwds_job_role_counts).is_not_null()
                    )
                )
            ),
            brief="ascwds_job_role_counts must be null where job_role_filtering_rule is not populated",
        )
        .col_vals_lt(
            pre=count_nulls,
            columns=IndCqcColumns.imputed_ascwds_job_role_counts,
            value=pb.col(IndCqcColumns.ascwds_job_role_counts),
        )
        .col_vals_lt(
            pre=count_nulls,
            columns=IndCqcColumns.imputed_ascwds_job_role_ratios,
            value=pb.col(IndCqcColumns.ascwds_job_role_ratios),
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
            brief="ascwds_job_role_counts and ascwds_job_role_ratios must be not null per row",
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
            brief="imputed_ascwds_job_role_counts and imputed_ascwds_job_role_ratios must be not null per row",
        )
        .col_vals_gt(
            columns=IndCqcColumns.estimate_filled_posts,
            value=0,
            na_pass=True,
            brief="estimate_filled_posts should be > 0 where present",
        )
        .col_vals_ge(
            columns=[
                IndCqcColumns.ascwds_job_role_counts,
                IndCqcColumns.imputed_ascwds_job_role_counts,
                IndCqcColumns.ascwds_job_role_rolling_ratio,
            ],
            value=0,
            na_pass=True,
            brief="values should be >= 0 where present",
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
            brief="ascwds_job_role_rolling_ratio should be between 0 and 1 where present",
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
