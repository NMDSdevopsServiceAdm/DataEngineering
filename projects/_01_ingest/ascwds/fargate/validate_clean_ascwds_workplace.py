import sys

import pointblank as pb
import polars as pl

from polars_utils import utils
from polars_utils.validation import actions as vl
from polars_utils.validation.constants import GLOBAL_ACTIONS, GLOBAL_THRESHOLDS
from projects._01_ingest.ascwds.fargate.utils.clean_workplace_utils import (
    create_slv_schema,
)
from utils.column_names.cleaned_data_files.ascwds_workplace_cleaned import (
    AscwdsWorkplaceCleanedColumns as ASCWPClean,
)
from utils.column_values.categorical_columns_by_dataset import (
    ASCWDSWorkplaceCleanedCategoricalValues as CatValues,
)

columns = {
    ASCWPClean.organisation_id: "string",
    ASCWPClean.period: "string",
    ASCWPClean.establishment_id: "string",
    ASCWPClean.establishment_id_from_nmds: "string",
    ASCWPClean.parent_id: "string",
    ASCWPClean.nmds_id: "string",
    ASCWPClean.establishment_created_date: "Date",
    ASCWPClean.establishment_updated_date: "Date",
    ASCWPClean.master_update_date: "Date",
    ASCWPClean.last_logged_in_date: "Date",
    ASCWPClean.la_permission: "string",
    ASCWPClean.is_bulk_uploader: "string",
    ASCWPClean.is_parent: "string",
    ASCWPClean.parent_permission: "string",
    ASCWPClean.registration_type: "string",
    ASCWPClean.provider_id: "string",
    ASCWPClean.location_id: "string",
    ASCWPClean.establishment_type: "string",
    ASCWPClean.establishment_name: "string",
    ASCWPClean.address: "string",
    ASCWPClean.postcode: "string",
    ASCWPClean.region_id: "string",
    ASCWPClean.total_staff: "Int32",
    ASCWPClean.worker_records: "Int32",
    ASCWPClean.total_starters: "string",
    ASCWPClean.total_leavers: "string",
    ASCWPClean.total_vacancies: "string",
    ASCWPClean.main_service_id: "string",
    ASCWPClean.version: "string",
    ASCWPClean.ascwds_workplace_import_date: "Date",
    ASCWPClean.master_update_date_org: "Date",
    ASCWPClean.purge_date: "Date",
    ASCWPClean.data_last_amended_date: "Date",
    ASCWPClean.workplace_last_active_date: "Date",
}


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

    slv_columns = create_slv_schema([i for i in range(1, 53)])
    slv_columns = {
        k: "Int32" for k in slv_columns
    }  # polars schema has datatype pl.Int32, but pb schema requires "Int32"
    columns.update(slv_columns.items())  # Add job role columns.
    columns.update(
        {
            ASCWPClean.total_staff_bounded: "Int32",
            ASCWPClean.worker_records_bounded: "Int32",
        }
    )  # Add columns created after job role cols are joined.
    EXPECTED_SCHEMA = pb.Schema(columns)

    validation = (
        pb.Validate(
            data=source_df,
            label=f"Validation of {source_path}",
            thresholds=GLOBAL_THRESHOLDS,
            brief=True,
            actions=GLOBAL_ACTIONS,
        )
        # dataset schema
        # .col_schema_match(
        #     schema=EXPECTED_SCHEMA,
        #     brief="Dataset should match the expected schema",
        # )
        # index columns
        .rows_distinct(
            [ASCWPClean.establishment_id, ASCWPClean.ascwds_workplace_import_date]
        )
        # complete columns
        .col_vals_not_null(
            columns=[
                ASCWPClean.organisation_id,
                ASCWPClean.ascwds_workplace_import_date,
                ASCWPClean.establishment_id,
            ],
            brief="Key columns should contain no null values",
        )
        # numerical
        .col_vals_between(
            columns=[
                ASCWPClean.total_staff_bounded,
                ASCWPClean.worker_records_bounded,
            ],
            left=1,
            right=3000,
            na_pass=True,
            brief="Counts should be between 1 and 3000 where present.",
        )
        # categorical
        .col_vals_in_set(
            ASCWPClean.establishment_type,
            [*CatValues.establishment_type_column_values.categorical_values, None],
        )
        .col_vals_in_set(
            ASCWPClean.parent_permission,
            [
                *CatValues.parent_permission_column_values.categorical_values,
                None,
            ],
        )
        .col_vals_in_set(
            ASCWPClean.is_parent,
            CatValues.is_parent_column_values.categorical_values,
        )
        .col_vals_in_set(
            ASCWPClean.main_service_id,
            [*CatValues.main_service_id_column_values.categorical_values, None],
        )
        .col_vals_in_set(
            ASCWPClean.registration_type,
            [*CatValues.registration_type_column_values.categorical_values, None],
        )
        # distinct values
        .specially(
            vl.is_unique_count_equal(
                ASCWPClean.parent_permission,
                CatValues.parent_permission_column_values.count_of_categorical_values,
            ),
            brief=f"{ASCWPClean.parent_permission} should have exactly {CatValues.parent_permission_column_values.count_of_categorical_values} distinct values",
        )
        .specially(
            vl.is_unique_count_equal(
                ASCWPClean.is_parent,
                CatValues.is_parent_column_values.count_of_categorical_values,
            ),
            brief=f"{ASCWPClean.is_parent} should have exactly {CatValues.is_parent_column_values.count_of_categorical_values} distinct values",
        )
        .specially(
            vl.is_unique_count_equal(
                ASCWPClean.registration_type,
                CatValues.registration_type_column_values.count_of_categorical_values,
            ),
            brief=f"{ASCWPClean.registration_type} should have exactly {CatValues.registration_type_column_values.count_of_categorical_values} distinct values",
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
