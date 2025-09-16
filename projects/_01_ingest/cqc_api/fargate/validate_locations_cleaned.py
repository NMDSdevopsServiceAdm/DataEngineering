import sys

import pointblank as pb

from polars_utils import utils
from polars_utils.expressions import str_length_cols
from polars_utils.logger import get_logger
from polars_utils.validation import actions as vl
from polars_utils.validation.constants import GLOBAL_ACTIONS, GLOBAL_THRESHOLDS
from polars_utils.validation.rules.locations_cleaned import Rules
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys
from utils.column_names.raw_data_files.cqc_location_api_columns import (
    NewCqcLocationApiColumns as CQCL,
)

compare_columns_to_import = [
    PartitionKeys.import_date,
    CQCL.location_id,
    CQCL.provider_id,
    CQCL.type,
    CQCL.registration_status,
    CQCL.gac_service_types,
    CQCL.regulated_activities,
]

logger = get_logger(__name__)


def main(
    bucket_name: str, source_path: str, reports_path: str, compare_path: str
) -> None:
    """Validates a dataset according to a set of provided rules and produces a summary report as well as failure outputs.

    Args:
        bucket_name (str): the bucket (name only) in which to source the dataset and output the report to
            - shoud correspond to workspace / feature branch name
        source_path (str): the source dataset path to be validated
        reports_path (str): the output path to write reports to
        compare_path (str): optional path to a dataset to compare against for expected size
    """
    source_df = utils.read_parquet(
        f"s3://{bucket_name}/{source_path}", exclude_complex_types=True
    ).with_columns(
        str_length_cols([CQCL.location_id, CQCL.provider_id]),
    )
    compare_df = utils.read_parquet(
        f"s3://{bucket_name}/{compare_path}",
        selected_columns=compare_columns_to_import,
    )

    validation = (
        pb.Validate(
            data=source_df,
            label=f"Validation of {source_path}",
            thresholds=GLOBAL_THRESHOLDS,
            brief=True,
            actions=GLOBAL_ACTIONS,
        )
        # dataset size
        .col_count_match(Rules.expected_size(compare_df))
        # complete columns
        .col_vals_not_null(*Rules.complete_columns)
        # index columns
        .rows_distinct(*Rules.index_columns)
        # min values
        .col_vals_ge(*Rules.time_registered,)
        # between (inclusive)
        .col_vals_between(*Rules.number_of_beds, na_pass=True)
        .col_vals_between(*Rules.location_id_length)
        .col_vals_between(*Rules.provider_id_length)
        # categorical
        .col_vals_in_set(*Rules.care_home)
        .col_vals_in_set(*Rules.cqc_sector)
        .col_vals_in_set(*Rules.registration_status)
        .col_vals_in_set(*Rules.dormancy)
        .col_vals_in_set(*Rules.primary_service_type)
        .col_vals_in_set(*Rules.contemporary_cssr)
        .col_vals_in_set(*Rules.contemporary_region)
        .col_vals_in_set(*Rules.current_cssr)
        .col_vals_in_set(*Rules.current_region)
        .col_vals_in_set(*Rules.current_rural_urban_ind_11)
        .col_vals_in_set(*Rules.related_location)
        # distinct values
        # TODO: Improve logging and report info for `specially` validations
        .specially(vl.is_unique_count_equal(*Rules.care_home_count))
        .specially(vl.is_unique_count_equal(*Rules.cqc_sector_count))
        .specially(vl.is_unique_count_equal(*Rules.registration_status_count))
        .specially(vl.is_unique_count_equal(*Rules.dormancy_count))
        .specially(vl.is_unique_count_equal(*Rules.primary_service_type_count))
        .specially(vl.is_unique_count_equal(*Rules.contemporary_cssr_count))
        .specially(vl.is_unique_count_equal(*Rules.contemporary_region_count))
        .specially(vl.is_unique_count_equal(*Rules.current_cssr_count))
        .specially(vl.is_unique_count_equal(*Rules.current_region_count))
        .specially(vl.is_unique_count_equal(*Rules.current_rural_urban_ind_11_count))
        .specially(vl.is_unique_count_equal(*Rules.related_location_count))
        # custom validation
        .col_vals_expr(Rules.custom_type())
        # run all rules
        .interrogate()
    )
    vl.write_reports(validation, bucket_name, reports_path)


if __name__ == "__main__":
    logger.info(f"Validation script called with parameters: {sys.argv}")

    args = utils.get_args(
        ("--bucket_name", "S3 bucket for source dataset and validation report"),
        ("--source_path", "The filepath of the dataset to validate"),
        ("--reports_path", "The filepath to output reports"),
        ("--compare_path", "The filepath to output reports"),
    )
    logger.info(f"Starting validation for {args.source_path}")

    main(args.bucket_name, args.source_path, args.reports_path, args.compare_path)
    logger.info(f"Validation of {args.source_path} complete")
