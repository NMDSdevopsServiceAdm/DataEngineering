import sys
from datetime import datetime

import pointblank as pb
import polars as pl

from polars_utils import utils
from polars_utils.validation import actions as vl
from polars_utils.validation.constants import GLOBAL_ACTIONS, GLOBAL_THRESHOLDS
from projects._04_direct_payment_recipients.direct_payments_config_polars import (
    DirectPaymentConfiguration as Config,
)
from utils.column_names.direct_payments_column_names import (
    DirectPaymentColumnNames as DP,
)
from utils.column_values.categorical_columns_by_dataset import (
    DirectPaymentRecipientsEstimateCategoricalValues as DPCatValues,
)
from utils.column_values.categorical_columns_by_dataset import (
    PostcodeDirectoryCleanedCategoricalValues as CatValues,
)

# Estimates before this year draw on incomplete/no survey data, so completeness
# is only checked from it. Not a general safety cutoff though: the (per-LA-area)
# extrapolation ratio can still produce an unbounded value from this year on -
# see the completeness-only checks below for those columns.
FIRST_YEAR_WITH_COMPLETE_ESTIMATES = 2015


def filter_to_complete_estimate_years(df: pl.DataFrame) -> pl.DataFrame:
    """Filters to the years in which the estimated proportions are expected.

    Args:
        df (pl.DataFrame): the dataset being validated.

    Returns:
        pl.DataFrame: rows from `FIRST_YEAR_WITH_COMPLETE_ESTIMATES` onwards.
    """
    return df.filter(pl.col(DP.YEAR_AS_INTEGER) >= FIRST_YEAR_WITH_COMPLETE_ESTIMATES)


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
    isles_of_scilly = "Isles of Scilly"
    source_df = utils.read_parquet(
        f"s3://{bucket_name}/{source_path}", exclude_complex_types=True
    )
    compare_df = utils.read_parquet(f"s3://{bucket_name}/{compare_path}")
    expected_row_count = compare_df.filter(pl.col(DP.LA_AREA) != isles_of_scilly).height

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
            brief=f"Merged DPR data file has {source_df.height} rows but expecting {expected_row_count} rows",
        )
        # distinct rows
        .rows_distinct(
            [DP.LA_AREA, DP.YEAR_AS_INTEGER],
            brief=f"Duplicate rows found for {DP.LA_AREA} and {DP.YEAR_AS_INTEGER}",
        )
        # complete columns
        .col_vals_not_null(
            [DP.YEAR_AS_INTEGER, DP.LA_AREA],
        )
        .col_vals_not_null(
            [
                DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF,
                DP.ROLLING_AVERAGE_ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF,
            ],
            pre=filter_to_complete_estimate_years,
            brief=f"Estimated proportions should be complete from {FIRST_YEAR_WITH_COMPLETE_ESTIMATES}",
        )
        # categorical
        .col_vals_in_set(
            DP.LA_AREA,
            CatValues.contemporary_cssr_column_values.categorical_values,
        )
        .col_vals_in_set(
            DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF_SOURCE,
            DPCatValues.estimated_proportion_of_service_users_employing_staff_source_column_values.categorical_values,
            pre=filter_to_complete_estimate_years,
        )
        # distinct values
        .specially(
            vl.is_unique_count_equal(
                DP.LA_AREA,
                CatValues.contemporary_cssr_column_values.count_of_categorical_values,
            ),
            brief=f"{DP.LA_AREA} needs to be one of {CatValues.contemporary_cssr_column_values.categorical_values} or {CatValues.current_cssr_column_values.categorical_values}",
        )
        .col_vals_between(
            DP.FIRST_YEAR_WITH_DATA,
            Config.FIRST_YEAR,
            datetime.now().year,
            na_pass=True,
        )
        .col_vals_between(
            DP.LAST_YEAR_WITH_DATA,
            Config.FIRST_YEAR,
            datetime.now().year,
            na_pass=True,
        )
        # numeric - proportions: interpolation stays within 0-1 because
        # remove_outliers.py nulls raw values outside it. The mean is coalesced
        # with an unbounded historic estimate, so - like the estimated proportion
        # and rolling average - it's left unbounded, only checked for completeness.
        .col_vals_between(
            DP.ESTIMATE_USING_INTERPOLATION,
            0.0,
            1.0,
            na_pass=True,
            pre=filter_to_complete_estimate_years,
        )
        .col_vals_ge(
            DP.ESTIMATED_SERVICE_USER_DPRS_DURING_YEAR_EMPLOYING_STAFF,
            0.0,
            na_pass=True,
        )
        .col_vals_ge(
            DP.ESTIMATED_SERVICE_USERS_WITH_SELF_EMPLOYED_STAFF, 0.0, na_pass=True
        )
        .col_vals_ge(DP.ESTIMATED_TOTAL_DPR_EMPLOYING_STAFF, 0.0, na_pass=True)
        .col_vals_ge(
            DP.ESTIMATED_TOTAL_PERSONAL_ASSISTANT_FILLED_POSTS, 0.0, na_pass=True
        )
        .col_vals_ge(
            DP.ESTIMATED_PROPORTION_OF_TOTAL_DPR_EMPLOYING_STAFF, 0.0, na_pass=True
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
        (
            "--compare_path",
            "The filepath to a dataset to compare against for expected size",
        ),
    )
    print(f"Starting validation for {args.source_path}")

    main(args.bucket_name, args.source_path, args.reports_path, args.compare_path)
    print(f"Validation of {args.source_path} complete")
