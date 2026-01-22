import sys

import pointblank as pb

from polars_utils import utils
from polars_utils.expressions import str_length_cols
from polars_utils.validation import actions as vl
from polars_utils.validation.constants import GLOBAL_ACTIONS, GLOBAL_THRESHOLDS
from projects._03_independent_cqc._04_model.utils.validate_models import (
    get_expected_row_count_for_model_features,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.ind_cqc_pipeline_columns import ModelRegistryKeys as MRKeys
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys
from utils.column_names.validation_table_columns import Validation


def main(
    bucket_name: str,
    source_path: str,
    reports_path: str,
    compare_path: str,
    model: str,
) -> None:
    """Validates a dataset according to a set of provided rules and produces a summary report as well as failure outputs.

    Args:
        bucket_name (str): the bucket (name only) in which to source the dataset and output the report to
            - shoud correspond to workspace / feature branch name
        source_path (str): the source dataset path to be validated
        reports_path (str): the output path to write reports to
        compare_path (str): path to a dataset to compare against for expected size
        model (str): the model for which the data have been prepared
    """
    source_df = utils.read_parquet(
        f"s3://{bucket_name}/{source_path}", exclude_complex_types=False
    ).with_columns(
        str_length_cols([IndCQC.location_id]),
    )

    compare_df = utils.read_parquet(
        f"s3://{bucket_name}/{compare_path}",
    )

    expected_row_count = get_expected_row_count_for_model_features(compare_df, model)
    not_null_cols = source_df.columns
    not_null_cols.remove(IndCQC.imputed_filled_post_model)

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
        .col_vals_not_null(not_null_cols)
        # incomplete column exists
        .col_exists(IndCQC.imputed_filled_post_model)
        # index columns
        .rows_distinct(
            [
                IndCQC.location_id,
                IndCQC.cqc_location_import_date,
                Keys.import_date,
            ]
        )
        # numeric column values are between (inclusive)
        .col_vals_between(Validation.location_id_length, 3, 14).interrogate()
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
        (
            "--model",
            "The model for which the data have been prepared",
        ),
    )
    print(f"Starting validation for {args.source_path}")

    main(
        args.bucket_name,
        args.source_path,
        args.reports_path,
        args.compare_path,
        args.model,
    )
    print(f"Validation of {args.source_path} complete")
