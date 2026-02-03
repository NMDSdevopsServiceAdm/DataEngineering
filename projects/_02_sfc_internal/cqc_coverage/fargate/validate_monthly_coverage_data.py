import sys

import pointblank as pb
import polars as pl

from polars_utils import utils
from polars_utils.expressions import has_value, str_length_cols
from polars_utils.validation import actions as vl
from polars_utils.validation.constants import GLOBAL_ACTIONS, GLOBAL_THRESHOLDS
from utils.column_names.coverage_columns import CoverageColumns
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns
from utils.column_names.cleaned_data_files.ascwds_workplace_cleaned import (
    AscwdsWorkplaceCleanedColumns as AscWdsColumns,
)
from utils.column_names.reconciliation_columns import ReconciliationColumns
from utils.column_names.cqc_ratings_columns import CQCRatingsColumns
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys
from utils.column_names.validation_table_columns import Validation
from utils.column_values.categorical_columns_by_dataset import (
    LocationsApiCleanedCategoricalValues as CatValues,
)


def main(
    bucket_name: str, source_path: str, reports_path: str, compare_path: str
) -> None:
    """Validates a dataset according to a set of provided rules and produces a summary report as well as failure outputs.

    Args:
        bucket_name (str): the bucket (name only) in which to source the dataset and output the report to
            - shoud correspond to workspace / feature branch name
        source_path (str): the source dataset path to be validated
        reports_path (str): the output path to write reports to
        compare_path (str): path to a dataset to compare against for expected size
    """
    source_df = utils.read_parquet(
        f"s3://{bucket_name}/{source_path}", exclude_complex_types=True
    ).with_columns(
        str_length_cols([IndCqcColumns.location_id, IndCqcColumns.provider_id]),
    )
    compare_df = utils.read_parquet(f"s3://{bucket_name}/{compare_path}")
    expected_row_count = calculate_expected_size_of_merged_coverage_dataset(compare_df)

    validation = (
        pb.Validate(
            data=source_df,
            label=f"Validation of {source_path}",
            thresholds=GLOBAL_THRESHOLDS,
            brief=True,
            actions=GLOBAL_ACTIONS,
        )
        # dataset size
        .row_count_match(
            expected_row_count,
            brief=f"Cleaned file has {source_df.height} rows but expecting {expected_row_count} rows",
        )
        # complete columns
        .col_vals_not_null(
            [
                IndCqcColumns.location_id,
                IndCqcColumns.name,
                IndCqcColumns.cqc_location_import_date,
                IndCqcColumns.care_home,
                IndCqcColumns.provider_id,
                IndCqcColumns.cqc_sector,
                IndCqcColumns.imputed_registration_date,
                IndCqcColumns.primary_service_type,
                IndCqcColumns.current_ons_import_date,
                IndCqcColumns.postcode,
                IndCqcColumns.current_cssr,
                IndCqcColumns.current_region,
                IndCqcColumns.current_rural_urban_indicator_2011,
                CoverageColumns.in_ascwds,
                CoverageColumns.la_monthly_coverage,
                CoverageColumns.locations_monthly_change,
                CoverageColumns.new_registrations_monthly,
                CoverageColumns.new_registrations_ytd,
            ]
        )
        # incomplete column exists
        .col_exists(
            [
                AscWdsColumns.master_update_date,
                AscWdsColumns.nmds_id,
                IndCqcColumns.dormancy,
                CQCRatingsColumns.overall_rating,
                IndCqcColumns.provider_name,
                ReconciliationColumns.parents_or_singles_and_subs,
                CoverageColumns.coverage_monthly_change,
                AscWdsColumns.last_logged_in_date,
            ]
        )
        # index columns
        .rows_distinct(
            [
                IndCqcColumns.location_id,
                IndCqcColumns.cqc_location_import_date,
            ],
        )
        # greater than or equal to
        .col_vals_ge(CoverageColumns.new_registrations_monthly, 0, na_pass=True)
        .col_vals_ge(CoverageColumns.new_registrations_ytd, 0, na_pass=True)
        # between (inclusive)
        .col_vals_between(Validation.location_id_length, 3, 14)
        .col_vals_between(Validation.provider_id_length, 3, 14)
        .col_vals_between(CoverageColumns.in_ascwds, 0, 1)
        .col_vals_between(CoverageColumns.la_monthly_coverage, 0, 1)
        .col_vals_between(CoverageColumns.coverage_monthly_change, -1, 1)
        # categorical
        .col_vals_in_set(
            IndCqcColumns.care_home,
            CatValues.care_home_column_values.categorical_values,
        )
        .col_vals_in_set(
            IndCqcColumns.current_cssr,
            CatValues.current_cssr_column_values.categorical_values,
        )
        .col_vals_in_set(
            IndCqcColumns.current_region,
            CatValues.current_region_column_values.categorical_values,
        )
        .col_vals_in_set(
            IndCqcColumns.current_rural_urban_indicator_2011,
            CatValues.current_rui_column_values.categorical_values,
        )
        .col_vals_in_set(
            IndCqcColumns.cqc_sector,
            CatValues.sector_column_values.categorical_values,
        )
        .col_vals_in_set(
            IndCqcColumns.dormancy,
            CatValues.dormancy_column_values.categorical_values,
        )
        .col_vals_in_set(
            IndCqcColumns.primary_service_type,
            CatValues.primary_service_type_column_values.categorical_values,
        )
        # distinct values
        .specially(
            vl.is_unique_count_equal(
                IndCqcColumns.care_home,
                CatValues.care_home_column_values.count_of_categorical_values,
            ),
            brief=f"{IndCqcColumns.care_home} needs to be one of {CatValues.care_home_column_values.categorical_values}",
        )
        .specially(
            vl.is_unique_count_equal(
                IndCqcColumns.current_cssr,
                CatValues.current_cssr_column_values.count_of_categorical_values,
            ),
            brief=f"{IndCqcColumns.current_cssr} needs to be one of {CatValues.current_cssr_column_values.categorical_values}",
        )
        .specially(
            vl.is_unique_count_equal(
                IndCqcColumns.current_region,
                CatValues.current_region_column_values.count_of_categorical_values,
            ),
            brief=f"{IndCqcColumns.current_region} needs to be one of {CatValues.current_region_column_values.categorical_values}",
        )
        .specially(
            vl.is_unique_count_equal(
                IndCqcColumns.current_rural_urban_indicator_2011,
                CatValues.current_rui_column_values.count_of_categorical_values,
            ),
            brief=f"{IndCqcColumns.current_rural_urban_indicator_2011} needs to be one of {CatValues.current_rui_column_values.categorical_values}",
        )
        .specially(
            vl.is_unique_count_equal(
                IndCqcColumns.cqc_sector,
                CatValues.sector_column_values.count_of_categorical_values,
            ),
            brief=f"{IndCqcColumns.cqc_sector} needs to be one of {CatValues.sector_column_values.categorical_values}",
        )
        .specially(
            vl.is_unique_count_equal(
                IndCqcColumns.dormancy,
                CatValues.dormancy_column_values.count_of_categorical_values,
            ),
            brief=f"{IndCqcColumns.dormancy} needs to be one of {CatValues.dormancy_column_values.categorical_values}",
        )
        .specially(
            vl.is_unique_count_equal(
                IndCqcColumns.primary_service_type,
                CatValues.primary_service_type_column_values.count_of_categorical_values,
            ),
            brief=f"{IndCqcColumns.primary_service_type} needs to be one of {CatValues.primary_service_type_column_values.categorical_values}",
        )
        .interrogate()
    )
    vl.write_reports(validation, bucket_name, reports_path)


def calculate_expected_size_of_merged_coverage_dataset(
    df: pl.DataFrame,
) -> int:
    df = df.unique(
        subset=[
            IndCqcColumns.cqc_location_import_date,
            IndCqcColumns.name,
            IndCqcColumns.postcode,
            IndCqcColumns.care_home,
        ]
    )
    df = reduce_dataset_to_earliest_file_per_month(df)
    expected_size = df.count()
    return expected_size


def reduce_dataset_to_earliest_file_per_month(df: pl.DataFrame) -> pl.DataFrame:
    """
    Reduce the dataset to the first file of every month.

    This function identifies the date of the first import date in each month and then filters the dataset to those import dates only.

    Args:
        df (pl.DataFrame): A dataframe containing the partition keys year, month and day.

    Returns:
        pl.DataFrame: A dataframe with only the first import date of each month.
    """
    earliest_day_in_month = "first_day_in_month"
    df = (
        df.with_columns(
            pl.col(Keys.day)
            .min()
            .over([Keys.year, Keys.month])
            .alias(earliest_day_in_month)
        )
        .filter(pl.col(earliest_day_in_month) == pl.col(Keys.day))
        .drop(earliest_day_in_month)
    )
    return df


if __name__ == "__main__":
    print(f"Validation script called with parameters: {sys.argv}")

    args = utils.get_args(
        ("--bucket_name", "S3 bucket for source dataset and validation report"),
        ("--source_path", "The filepath of the dataset to validate"),
        ("--reports_path", "The filepath to output reports"),
        (
            "--compare_path",
            "The filepath to a dataset to compare against for expected size",
        ),
    )
    print(f"Starting validation for {args.source_path}")

    main(args.bucket_name, args.source_path, args.reports_path, args.compare_path)
    print(f"Validation of {args.source_path} complete")
