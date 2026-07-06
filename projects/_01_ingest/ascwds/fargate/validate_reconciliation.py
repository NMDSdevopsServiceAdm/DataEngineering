import sys

import pointblank as pb

from polars_utils import utils
from polars_utils.validation import actions as vl
from polars_utils.validation.constants import GLOBAL_ACTIONS, GLOBAL_THRESHOLDS
from utils.column_names.cleaned_data_files.ascwds_workplace_cleaned import (
    AscwdsWorkplaceCleanedColumns as AWPClean,
)
from utils.column_values.categorical_columns_by_dataset import (
    ASCWDSWorkplaceCleanedCategoricalValues as CatValues,
)

EXPECTED_SCHEMA = pb.Schema(
    columns={
        AWPClean.ascwds_workplace_import_date: "Date",
        AWPClean.establishment_id: "String",
        AWPClean.nmds_id: "String",
        AWPClean.master_update_date: "Date",
        AWPClean.master_update_date_org: "Date",
        AWPClean.establishment_created_date: "Date",
        AWPClean.is_parent: "String",
        AWPClean.parent_id: "String",
        AWPClean.organisation_id: "String",
        AWPClean.parent_permission: "String",
        AWPClean.establishment_type: "String",
        AWPClean.registration_type: "String",
        AWPClean.location_id: "String",
        AWPClean.main_service_id: "String",
        AWPClean.establishment_name: "String",
        AWPClean.region_id: "String",
        AWPClean.total_staff: "Int32",
        AWPClean.worker_records: "Int32",
        AWPClean.last_logged_in_date: "Date",
        AWPClean.la_permission: "String",
    }
)


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
        .rows_distinct(
            [AWPClean.establishment_id, AWPClean.ascwds_workplace_import_date]
        )
        # complete columns
        .col_vals_not_null(
            columns=[
                AWPClean.organisation_id,
                AWPClean.ascwds_workplace_import_date,
                AWPClean.establishment_id,
            ],
            brief="Key columns should contain no null values",
        )
        # categorical
        # .col_vals_in_set(
        #     AWPClean.establishment_type,
        #     [*CatValues.establishment_type_column_values.categorical_values, None],
        # )
        # .col_vals_in_set(
        #     AWPClean.parent_permission,
        #     [*CatValues.parent_permission_column_values.categorical_values, None],
        # )
        .col_vals_in_set(
            AWPClean.is_parent,
            [*CatValues.is_parent_column_values.categorical_values, None],
        )
        # .col_vals_in_set(
        #     AWPClean.main_service_id,
        #     [*CatValues.main_service_id_column_values.categorical_values, None],
        # )
        # .col_vals_in_set(
        #     AWPClean.registration_type,
        #     [*CatValues.registration_type_column_values.categorical_values, None],
        # )
        # distinct values
        # .specially(
        #     vl.is_unique_count_equal(
        #         AWPClean.establishment_type,
        #         CatValues.establishment_type_column_values.count_of_categorical_values,
        #     ),
        #     brief=f"{AWPClean.establishment_type} should have exactly {CatValues.establishment_type_column_values.count_of_categorical_values} distinct values",
        # )
        # .specially(
        #     vl.is_unique_count_equal(
        #         AWPClean.parent_permission,
        #         CatValues.parent_permission_column_values.count_of_categorical_values,
        #     ),
        #     brief=f"{AWPClean.parent_permission} should have exactly {CatValues.parent_permission_column_values.count_of_categorical_values} distinct values",
        # )
        .specially(
            vl.is_unique_count_equal(
                AWPClean.is_parent,
                CatValues.is_parent_column_values.count_of_categorical_values,
            ),
            brief=f"{AWPClean.is_parent} should have exactly {CatValues.is_parent_column_values.count_of_categorical_values} distinct values",
        )
        # .specially(
        #     vl.is_unique_count_equal(
        #         AWPClean.main_service_id,
        #         CatValues.main_service_id_column_values.count_of_categorical_values,
        #     ),
        #     brief=f"{AWPClean.main_service_id} should have exactly {CatValues.main_service_id_column_values.count_of_categorical_values} distinct values",
        # )
        # .specially(
        #     vl.is_unique_count_equal(
        #         AWPClean.registration_type,
        #         CatValues.registration_type_column_values.count_of_categorical_values,
        #     ),
        #     brief=f"{AWPClean.registration_type} should have exactly {CatValues.registration_type_column_values.count_of_categorical_values} distinct values",
        # )
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
