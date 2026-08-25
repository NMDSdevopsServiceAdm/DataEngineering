import sys

import pointblank as pb

from polars_utils import utils
from polars_utils.column_types import CategoricalColumnTypes
from polars_utils.validation import actions as vl
from polars_utils.validation.constants import GLOBAL_ACTIONS, GLOBAL_THRESHOLDS
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns
from utils.column_values.categorical_columns_by_dataset import (
    EstimatedIndCQCFilledPostsByJobRoleCategoricalValues as CatValues,
)

VALIDATION_COLS_TO_IMPORT = [
    IndCqcColumns.cqc_location_import_date,
    # IndCqcColumns.current_region,
    IndCqcColumns.primary_service_type,
    IndCqcColumns.main_job_role_clean_labelled,
    IndCqcColumns.main_job_group_labelled,
    IndCqcColumns.estimate_filled_posts_by_job_role_historically_reallocated,
]

COMPARE_COLS_TO_IMPORT = [
    IndCqcColumns.estimate_filled_posts_by_job_role_historically_reallocated,
]

EXPECTED_SCHEMA = pb.Schema(
    columns={
        IndCqcColumns.cqc_location_import_date: "Date",
        # IndCqcColumns.current_region: "String",
        IndCqcColumns.primary_service_type: str(
            CategoricalColumnTypes.PrimaryServiceEnumType
        ),
        IndCqcColumns.main_job_role_clean_labelled: str(
            CategoricalColumnTypes.JobRoleCatType
        ),
        IndCqcColumns.estimate_filled_posts_by_job_role_historically_reallocated: "Float64",
        IndCqcColumns.main_job_group_labelled: str(
            CategoricalColumnTypes.JobGroupCatType
        ),
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

    # compare_df = utils.read_parquet(
    #     source=f"s3://{bucket_name}/{compare_path}",
    #     selected_columns=COMPARE_COLS_TO_IMPORT,
    # )

    # compare_estimate_filled_posts_sum = compare_df[
    #     IndCqcColumns.estimate_filled_posts_by_job_role_historically_reallocated
    # ].sum()

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
        # complete columns
        .col_vals_not_null(
            [
                IndCqcColumns.cqc_location_import_date,
                # IndCqcColumns.current_region,
                IndCqcColumns.primary_service_type,
                IndCqcColumns.main_job_role_clean_labelled,
            ]
        )
        # Cross-dataset aggregate check
        # .col_vals_expr(
        #     expr=(
        #         pl.col(
        #             IndCqcColumns.estimate_filled_posts_by_job_role_historically_reallocated
        #         ).sum()
        #         < compare_estimate_filled_posts_sum
        #     ),
        #     brief=(
        #         f"Total sum of estimate_filled_posts_by_job_role_historically_reallocated in clean job should be less than the total sum of estimate_filled_posts_by_job_role_historically_reallocated in the merge job dataset"
        #     ),
        # )
        # numerical
        .col_vals_ge(
            columns=IndCqcColumns.estimate_filled_posts_by_job_role_historically_reallocated,
            value=0,
            brief="estimate_filled_posts_by_job_role_historically_reallocated should be >= 0 where present",
        )
        # categorical
        # .col_vals_in_set(
        #     IndCqcColumns.current_region,
        #     CatValues.current_region_column_values.categorical_values,
        # )
        .col_vals_in_set(
            IndCqcColumns.primary_service_type,
            CatValues.primary_service_type_column_values.categorical_values,
        )
        .col_vals_in_set(
            IndCqcColumns.main_job_role_clean_labelled,
            CatValues.main_job_role_labels_column_values.categorical_values,
        )
        # .specially(
        #     vl.is_unique_count_equal(
        #         IndCqcColumns.current_region,
        #         CatValues.current_region_column_values.count_of_categorical_values,
        #     ),
        #     brief=f"{IndCqcColumns.current_region} should have exactly {CatValues.current_region_column_values.count_of_categorical_values} distinct values",
        # )
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
