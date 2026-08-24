import sys

import pointblank as pb
import polars as pl

from polars_utils import utils
from polars_utils.column_types import CategoricalColumnTypes
from polars_utils.validation import actions as vl
from polars_utils.validation.constants import GLOBAL_ACTIONS, GLOBAL_THRESHOLDS
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns, PartitionKeys
from utils.column_values.categorical_columns_by_dataset import (
    EstimatedIndCQCFilledPostsByJobRoleCategoricalValues as CatValues,
)

INDEX_VALIDATION_COLS_TO_IMPORT = [
    IndCqcColumns.id_per_locationid_import_date,
    IndCqcColumns.id_per_locationid_import_date_job_role,
    IndCqcColumns.location_id,
    IndCqcColumns.cqc_location_import_date,
    IndCqcColumns.main_job_role_clean_labelled,
]

OTHER_VALIDATION_COLS_TO_IMPORT = [
    IndCqcColumns.id_per_locationid_import_date_job_role,
    IndCqcColumns.location_id,
    IndCqcColumns.cqc_location_import_date,
    IndCqcColumns.primary_service_type,
    IndCqcColumns.id_per_locationid_import_date,
    IndCqcColumns.main_job_role_clean_labelled,
    IndCqcColumns.estimate_filled_posts_by_job_role_historically_reallocated,
    IndCqcColumns.main_job_group_labelled,
    PartitionKeys.year,
]

COMPARE_COLS_TO_IMPORT = [
    IndCqcColumns.location_id,
    IndCqcColumns.cqc_location_import_date,
]

EXPECTED_SCHEMA = pb.Schema(
    columns={
        IndCqcColumns.id_per_locationid_import_date_job_role: "UInt32",
        IndCqcColumns.location_id: str(CategoricalColumnTypes.LocationCatType),
        IndCqcColumns.cqc_location_import_date: "Date",
        IndCqcColumns.primary_service_type: str(
            CategoricalColumnTypes.PrimaryServiceEnumType
        ),
        IndCqcColumns.id_per_locationid_import_date: "UInt32",
        IndCqcColumns.main_job_role_clean_labelled: str(
            CategoricalColumnTypes.JobRoleCatType
        ),
        IndCqcColumns.estimate_filled_posts_by_job_role_historically_reallocated: "Float64",
        IndCqcColumns.main_job_group_labelled: str(
            CategoricalColumnTypes.JobGroupCatType
        ),
        PartitionKeys.year: "Int64",
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
        compare_path (str): the path to the comparison dataset
        reports_path (str): the output path to write reports to
    """
    index_validation(bucket_name, source_path, reports_path)
    other_validation(bucket_name, source_path, compare_path, reports_path)


def index_validation(bucket_name: str, source_path: str, reports_path: str) -> None:
    source_df = utils.read_parquet(
        source=f"s3://{bucket_name}/{source_path}",
        selected_columns=INDEX_VALIDATION_COLS_TO_IMPORT,
    )

    validation = (
        pb.Validate(
            data=source_df,
            label=f"Validation of {source_path}",
            thresholds=GLOBAL_THRESHOLDS,
            brief=True,
            actions=GLOBAL_ACTIONS,
        )
        .rows_distinct(
            columns_subset=IndCqcColumns.id_per_locationid_import_date_job_role,
            brief="id_per_locationid_import_date_job_role should be unique",
        )
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
        .interrogate()
    )
    vl.write_reports(validation, bucket_name, f"{reports_path}index_validation/")


def other_validation(
    bucket_name: str, source_path: str, compare_path: str, reports_path: str
) -> None:
    source_df = utils.read_parquet(
        source=f"s3://{bucket_name}/{source_path}",
        selected_columns=OTHER_VALIDATION_COLS_TO_IMPORT,
    )

    compare_df = utils.read_parquet(
        source=f"s3://{bucket_name}/{compare_path}",
        selected_columns=COMPARE_COLS_TO_IMPORT,
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
                IndCqcColumns.primary_service_type,
                IndCqcColumns.id_per_locationid_import_date,
                IndCqcColumns.main_job_role_clean_labelled,
                IndCqcColumns.estimate_filled_posts_by_job_role_historically_reallocated,
                PartitionKeys.year,
            ],
            brief="Key columns should contain no null values",
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
    ).interrogate()
    vl.write_reports(validation, bucket_name, f"{reports_path}other_validation/")


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
