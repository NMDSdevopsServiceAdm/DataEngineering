import sys

import pointblank as pb
import polars as pl

from polars_utils import utils
from polars_utils.validation import actions as vl
from polars_utils.validation.constants import GLOBAL_ACTIONS, GLOBAL_THRESHOLDS
from utils.column_names.cleaned_data_files.ons_cleaned import (
    OnsCleanedColumns as ONSClean,
)
from utils.column_names.raw_data_files.ons_columns import (
    OnsPostcodeDirectoryColumns as ONS,
)
from utils.column_values.categorical_columns_by_dataset import (
    PostcodeDirectoryCleanedCategoricalValues as CatValues,
)

COMPLETE_COLUMNS = [
    ONSClean.postcode,
    ONSClean.contemporary_ons_import_date,
    ONSClean.contemporary_cssr,
    ONSClean.contemporary_region,
    ONSClean.current_ons_import_date,
    ONSClean.current_cssr,
    ONSClean.current_region,
    ONSClean.current_sub_icb,
    ONSClean.current_icb,
    ONSClean.current_icb_region,
    ONSClean.current_lsoa21,
    ONSClean.current_msoa21,
    ONSClean.current_rural_urban_ind_11,
    ONSClean.current_rural_urban_ind_21,
]


def main(
    bucket_name: str, source_path: str, reports_path: str, compare_path: str
) -> None:
    """Validates the cleaned ONS postcode directory dataset.

    Args:
        bucket_name (str): the bucket (name only) in which to source the
            dataset and output the report to (should correspond to
            workspace / feature branch name).
        source_path (str): the cleaned dataset path to be validated.
        reports_path (str): the output path to write reports to.
        compare_path (str): the raw postcode directory path to compare
            against for the dataset's expected row count.
    """
    source_df = utils.read_parquet(source=f"s3://{bucket_name}/{source_path}")

    compare_lf = utils.scan_parquet(
        f"s3://{bucket_name}/{compare_path}", selected_columns=[ONS.postcode]
    )
    expected_row_count = compare_lf.select(pl.len()).collect().item()

    validation = (
        pb.Validate(
            data=source_df,
            label=f"Validation of {source_path}",
            thresholds=GLOBAL_THRESHOLDS,
            brief=True,
            actions=GLOBAL_ACTIONS,
        )
        .row_count_match(
            expected_row_count,
            brief=f"Cleaned dataset should have {expected_row_count} rows, matching the raw postcode directory",
        )
        .col_vals_not_null(
            columns=COMPLETE_COLUMNS,
            brief="Key columns should contain no null values",
        )
        .rows_distinct(
            [ONSClean.postcode, ONSClean.contemporary_ons_import_date],
            brief=f"{ONSClean.postcode} and {ONSClean.contemporary_ons_import_date} together should be unique",
        )
        .col_vals_in_set(
            ONSClean.contemporary_cssr,
            CatValues.contemporary_cssr_column_values.categorical_values,
        )
        .col_vals_in_set(
            ONSClean.contemporary_region,
            CatValues.contemporary_region_column_values.categorical_values,
        )
        .col_vals_in_set(
            ONSClean.current_cssr,
            CatValues.current_cssr_column_values.categorical_values,
        )
        .col_vals_in_set(
            ONSClean.current_region,
            CatValues.current_region_column_values.categorical_values,
        )
        .col_vals_in_set(
            ONSClean.current_rural_urban_ind_11,
            CatValues.current_rui_column_values.categorical_values,
        )
        .col_vals_in_set(
            ONSClean.current_rural_urban_ind_21,
            CatValues.current_rui_21_column_values.categorical_values,
        )
        .specially(
            vl.is_unique_count_equal(
                ONSClean.contemporary_cssr,
                CatValues.contemporary_cssr_column_values.count_of_categorical_values,
            ),
            brief=f"{ONSClean.contemporary_cssr} should have exactly {CatValues.contemporary_cssr_column_values.count_of_categorical_values} distinct values",
        )
        .specially(
            vl.is_unique_count_equal(
                ONSClean.contemporary_region,
                CatValues.contemporary_region_column_values.count_of_categorical_values,
            ),
            brief=f"{ONSClean.contemporary_region} should have exactly {CatValues.contemporary_region_column_values.count_of_categorical_values} distinct values",
        )
        .specially(
            vl.is_unique_count_equal(
                ONSClean.current_cssr,
                CatValues.current_cssr_column_values.count_of_categorical_values,
            ),
            brief=f"{ONSClean.current_cssr} should have exactly {CatValues.current_cssr_column_values.count_of_categorical_values} distinct values",
        )
        .specially(
            vl.is_unique_count_equal(
                ONSClean.current_region,
                CatValues.current_region_column_values.count_of_categorical_values,
            ),
            brief=f"{ONSClean.current_region} should have exactly {CatValues.current_region_column_values.count_of_categorical_values} distinct values",
        )
        .specially(
            vl.is_unique_count_equal(
                ONSClean.current_rural_urban_ind_11,
                CatValues.current_rui_column_values.count_of_categorical_values,
            ),
            brief=f"{ONSClean.current_rural_urban_ind_11} should have exactly {CatValues.current_rui_column_values.count_of_categorical_values} distinct values",
        )
        .specially(
            vl.is_unique_count_equal(
                ONSClean.current_rural_urban_ind_21,
                CatValues.current_rui_21_column_values.count_of_categorical_values,
            ),
            brief=f"{ONSClean.current_rural_urban_ind_21} should have exactly {CatValues.current_rui_21_column_values.count_of_categorical_values} distinct values",
        )
        .interrogate()
    )
    vl.write_reports(validation, bucket_name, reports_path)


if __name__ == "__main__":
    print(f"Validation script called with parameters: {sys.argv}")

    args = utils.get_args(
        ("--bucket_name", "S3 bucket for source dataset and validation report"),
        ("--source_path", "The filepath of the cleaned dataset to validate"),
        ("--reports_path", "The filepath to output reports"),
        (
            "--compare_path",
            "The filepath to the raw dataset to compare against for expected size",
        ),
    )
    print(f"Starting validation for {args.source_path}")

    main(args.bucket_name, args.source_path, args.reports_path, args.compare_path)
    print(f"Validation of {args.source_path} complete")
