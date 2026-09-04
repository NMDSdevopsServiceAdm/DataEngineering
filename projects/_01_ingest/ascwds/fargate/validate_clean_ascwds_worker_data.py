import sys

import pointblank as pb

from polars_utils import utils
from polars_utils.column_types import CategoricalColumnTypes
from polars_utils.validation import actions as vl
from polars_utils.validation.constants import GLOBAL_ACTIONS, GLOBAL_THRESHOLDS
from utils.column_names.cleaned_data_files.ascwds_worker_cleaned import (
    AscwdsWorkerCleanedColumns as ASCWKClean,
)
from utils.column_values.categorical_columns_by_dataset import (
    ASCWDSWorkerCleanedCategoricalValues as CatValues,
)

columns = {
    ASCWKClean.location_id: "String",
    ASCWKClean.establishment_id: "String",
    ASCWKClean.worker_id: "String",
    ASCWKClean.main_job_role_id: "String",
    ASCWKClean.ascwds_worker_import_date: "Date",
    ASCWKClean.main_job_role_clean: str(CategoricalColumnTypes.MainJobRoleIdCatType),
    ASCWKClean.main_job_role_clean_labelled: str(CategoricalColumnTypes.JobRoleCatType),
}
EXPECTED_SCHEMA = pb.Schema(columns)


def main(bucket_name: str, source_path: str, reports_path: str) -> None:
    """Validates a dataset according to a set of provided rules and produces a
        summary report as well as failure outputs.

    Args:
        bucket_name (str): the bucket (name only) in which to source the dataset
            and output the report to (should correspond to workspace / feature
            branch name)
        source_path (str): the source dataset path to be validated
        reports_path (str): the output path to write reports to
    """
    source_df = utils.read_parquet(
        source=f"s3://{bucket_name}/{source_path}",
    )

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
            brief="Dataset should match the expected schema",
        )
        # index columns
        .rows_distinct([ASCWKClean.worker_id, ASCWKClean.ascwds_worker_import_date])
        # complete columns
        .col_vals_not_null(
            columns=[
                ASCWKClean.establishment_id,
                ASCWKClean.worker_id,
                ASCWKClean.main_job_role_clean,
                ASCWKClean.main_job_role_clean_labelled,
                ASCWKClean.ascwds_worker_import_date,
            ],
            brief="Key columns should contain no null values",
        )
        # categorical
        .col_vals_in_set(
            ASCWKClean.main_job_role_clean,
            CatValues.main_job_role_id_column_values.categorical_values,
        )
        .col_vals_in_set(
            ASCWKClean.main_job_role_clean_labelled,
            CatValues.main_job_role_labels_column_values.categorical_values,
        )
        # distinct values
        .specially(
            vl.is_unique_count_equal(
                ASCWKClean.main_job_role_clean,
                CatValues.main_job_role_id_column_values.count_of_categorical_values,
            ),
            brief=f"{ASCWKClean.main_job_role_clean} should have exactly {CatValues.main_job_role_id_column_values.count_of_categorical_values} distinct values",
        )
        .specially(
            vl.is_unique_count_equal(
                ASCWKClean.main_job_role_clean_labelled,
                CatValues.main_job_role_labels_column_values.count_of_categorical_values,
            ),
            brief=f"{ASCWKClean.main_job_role_clean_labelled} should have exactly {CatValues.main_job_role_labels_column_values.count_of_categorical_values} distinct values",
        )
        .interrogate()
    )
    vl.write_reports(validation, bucket_name, reports_path)


if __name__ == "__main__":
    print(f"Validation script called with parameters: {sys.argv}")

    args = utils.get_args(
        ("--bucket_name", "S3 bucket for source dataset and validation report"),
        ("--source_path", "The filepath of the dataset to validate"),
        ("--reports_path", "The filepath to output reports"),
    )
    print(f"Starting validation for {args.source_path}")

    main(args.bucket_name, args.source_path, args.reports_path)
    print(f"Validation of {args.source_path} complete")
