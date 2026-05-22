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

ind_cqc_merge_job_role_cols_to_import = [
    IndCqcColumns.id_per_locationid_import_date,
    IndCqcColumns.location_id,
    IndCqcColumns.cqc_location_import_date,
    IndCqcColumns.primary_service_type,
    IndCqcColumns.estimate_filled_posts,
    IndCqcColumns.estimate_filled_posts_source,
    IndCqcColumns.main_job_role_clean_labelled,
    IndCqcColumns.ascwds_filled_posts_dedup_clean,
    IndCqcColumns.ascwds_job_role_counts,
]

ind_cqc_estimates_cols_to_import = [
    IndCqcColumns.location_id,
    IndCqcColumns.cqc_location_import_date,
]

CQC_EARLIEST_IMPORT_DATE = date(2013, 3, 1)

EXPECTED_SCHEMA = pb.Schema(
    columns={
        IndCqcColumns.id_per_locationid_import_date: "String",
        IndCqcColumns.location_id: "String",
        IndCqcColumns.cqc_location_import_date: "Date",
        IndCqcColumns.primary_service_type: "String",
        IndCqcColumns.estimate_filled_posts: "Float64",
        IndCqcColumns.estimate_filled_posts_source: "String",
        IndCqcColumns.main_job_role_clean_labelled: "String",
        IndCqcColumns.ascwds_filled_posts_dedup_clean: "Float64",
        IndCqcColumns.ascwds_job_role_counts: "Float64",
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
        compare_path (str): the path to the dataset to compare against
        reports_path (str): the output path to write reports to
    """
    source_df = utils.read_parquet(
        source=f"s3://{bucket_name}/{source_path}",
        selected_columns=ind_cqc_merge_job_role_cols_to_import,
    )
    source_df = source_df.with_columns(
        pl.col(IndCqcColumns.main_job_role_clean_labelled).cast(pl.String)
    )
    compare_df = utils.read_parquet(
        source=f"s3://{bucket_name}/{compare_path}",
        selected_columns=ind_cqc_estimates_cols_to_import,
    )
    expected_row_count = compare_df.height * len(jobGroupDict.all_roles())

    validation = (
        pb.Validate(
            data=source_df,
            label=f"Validation of {source_path}",
            thresholds=GLOBAL_THRESHOLDS,
            brief=True,
            actions=GLOBAL_ACTIONS,
        )
        # Schema check
        .col_schema_match(
            EXPECTED_SCHEMA,
            in_order=False,
            brief="All columns should have the expected data types",
        )
        # dataset size
        .row_count_match(
            expected_row_count,
            brief=f"Estimates file has {source_df.height} rows but expecting {expected_row_count} rows",
        )
        # Composite primary key
        .rows_distinct(
            columns_subset=[
                IndCqcColumns.location_id,
                IndCqcColumns.cqc_location_import_date,
                IndCqcColumns.main_job_role_clean_labelled,
            ],
            brief=f"Primary key (location_id, cqc_location_import_date, main_job_role_clean_labelled) should be unique",
        )
        # id_per_locationid_import_date unique within each location/import-date pair
        .rows_distinct(
            columns_subset=[
                IndCqcColumns.location_id,
                IndCqcColumns.cqc_location_import_date,
                IndCqcColumns.id_per_locationid_import_date,
            ],
            brief=f"id_per_locationid_import_date should be unique per locationid and cqc_location_import_date combination",
        )
        # complete columns
        .col_vals_not_null(
            [
                IndCqcColumns.id_per_locationid_import_date,
                IndCqcColumns.location_id,
                IndCqcColumns.cqc_location_import_date,
                IndCqcColumns.primary_service_type,
                IndCqcColumns.estimate_filled_posts,
                IndCqcColumns.estimate_filled_posts_source,
                IndCqcColumns.main_job_role_clean_labelled,
            ],
            brief=f"Required columns should contain no null values",
        )
        # Numeric range — strictly positive (nulls allowed)
        .col_vals_gt(
            columns=[
                IndCqcColumns.estimate_filled_posts,
                IndCqcColumns.ascwds_filled_posts_dedup_clean,
            ],
            value=0,
            na_pass=True,
            brief=f"estimate_filled_posts and ascwds_filled_posts_deduplicated_clean should be > 0 where present",
        )
        # Numeric range — non-negative (nulls allowed)
        .col_vals_ge(
            columns=[IndCqcColumns.ascwds_job_role_counts],
            value=0,
            na_pass=True,
            brief=f"ascwds_job_role_counts should be >= 0 where present",
        )
        # Date plausibility
        .col_vals_ge(
            columns=[IndCqcColumns.cqc_location_import_date],
            value=CQC_EARLIEST_IMPORT_DATE,
            brief=f"cqc_location_import_date should not be before {CQC_EARLIEST_IMPORT_DATE.strftime('%d/%m/%Y')}",
        )
        # Cross-column numeric constraint
        .col_vals_expr(
            expr=(
                pl.col(IndCqcColumns.ascwds_job_role_counts).is_null()
                | pl.col(IndCqcColumns.estimate_filled_posts).is_null()
                | (
                    pl.col(IndCqcColumns.ascwds_job_role_counts)
                    <= pl.col(IndCqcColumns.estimate_filled_posts)
                )
            ),
            brief=f"ascwds_job_role_counts <= estimate_filled_posts where both are present",
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
            ASCWDSWorkerCatValues.main_job_role_labels_column_values.categorical_values,
        )
        # distinct values
        .specially(
            vl.is_unique_count_equal(
                IndCqcColumns.estimate_filled_posts_source,
                CatValues.estimate_filled_posts_source_column_values.count_of_categorical_values,
            ),
            brief=f"{IndCqcColumns.estimate_filled_posts_source} needs to be one of {CatValues.estimate_filled_posts_source_column_values.categorical_values}",
        )
        .specially(
            vl.is_unique_count_equal(
                IndCqcColumns.primary_service_type,
                CatValues.primary_service_type_column_values.count_of_categorical_values,
            ),
            brief=f"{IndCqcColumns.primary_service_type} needs to be one of {CatValues.primary_service_type_column_values.categorical_values}",
        )
        .specially(
            vl.is_unique_count_equal(
                IndCqcColumns.main_job_role_clean_labelled,
                ASCWDSWorkerCatValues.main_job_role_labels_column_values.count_of_categorical_values,
            ),
            brief=f"{IndCqcColumns.main_job_role_clean_labelled} needs to be one of {ASCWDSWorkerCatValues.main_job_role_labels_column_values.categorical_values}",
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
