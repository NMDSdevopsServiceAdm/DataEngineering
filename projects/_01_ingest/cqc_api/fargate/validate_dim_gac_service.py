import sys

import pointblank as pb
import polars as pl

from polars_utils import utils
from polars_utils.logger import get_logger
from polars_utils.validation import actions as vl
from polars_utils.validation.constants import GLOBAL_ACTIONS, GLOBAL_THRESHOLDS
from utils.column_names.cleaned_data_files.cqc_location_cleaned import (
    CqcLocationCleanedColumns as CQCLClean,
)
from utils.column_names.ind_cqc_pipeline_columns import DimensionPartitionKeys
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_values.categorical_column_values import CareHome, PrimaryServiceType
from utils.column_values.categorical_columns_by_dataset import (
    LocationsApiCleanedCategoricalValues as CatValues,
)

logger = get_logger(__name__)


def main(bucket_name: str, source_path: str, reports_path: str) -> None:
    """Validates a dataset according to a set of provided rules and produces a summary report as well as failure outputs.

    Args:
        bucket_name (str): the bucket (name only) in which to source the dataset and output the report to
            - shoud correspond to workspace / feature branch name
        source_path (str): the source dataset path to be validated
        reports_path (str): the output path to write reports to
    """
    source_df = utils.read_parquet(
        f"s3://{bucket_name}/{source_path}", exclude_complex_types=True
    )

    validation = (
        pb.Validate(
            data=source_df,
            label=f"Validation of {source_path}",
            thresholds=GLOBAL_THRESHOLDS,
            brief=True,
            actions=GLOBAL_ACTIONS,
        )
        # complete columns
        .col_vals_not_null(
            [
                CQCLClean.primary_service_type,
                CQCLClean.care_home,
            ]
        )
        # index columns
        .rows_distinct(
            [
                CQCLClean.location_id,
                DimensionPartitionKeys.import_date,
                DimensionPartitionKeys.last_updated,
            ]
        )
        # categorical
        .col_vals_in_set(
            CQCLClean.care_home, CatValues.care_home_column_values.categorical_values
        )
        .col_vals_in_set(
            CQCLClean.primary_service_type,
            CatValues.primary_service_type_column_values.categorical_values,
        )
        # distinct values
        .specially(
            vl.is_unique_count_equal(
                CQCLClean.care_home,
                CatValues.care_home_column_values.count_of_categorical_values,
            )
        )
        .specially(
            vl.is_unique_count_equal(
                CQCLClean.primary_service_type,
                CatValues.primary_service_type_column_values.count_of_categorical_values,
            )
        )
        # index columns
        .col_vals_expr(custom_type())
        # run all rules
        .interrogate()
    )
    vl.write_reports(validation, bucket_name, reports_path)


def custom_type() -> pl.Expr:
    # for readability
    care_home_col = pl.col(IndCQC.care_home)
    primary_service_col = pl.col(IndCQC.primary_service_type)

    care_home = CareHome.care_home
    not_care_home = CareHome.not_care_home
    PST = PrimaryServiceType

    return (
        (care_home_col == not_care_home) & (primary_service_col == PST.non_residential)
    ) | (
        (care_home_col == care_home)
        & (primary_service_col.is_in([PST.care_home_with_nursing, PST.care_home_only]))
    )


if __name__ == "__main__":
    logger.info(f"Validation script called with parameters: {sys.argv}")

    args = utils.get_args(
        ("--bucket_name", "S3 bucket for source dataset and validation report"),
        ("--source_path", "The filepath of the dataset to validate"),
        ("--reports_path", "The filepath to output reports"),
    )
    logger.info(f"Starting validation for {args.source_path}")

    main(args.bucket_name, args.source_path, args.reports_path)
    logger.info(f"Validation of {args.source_path} complete")
