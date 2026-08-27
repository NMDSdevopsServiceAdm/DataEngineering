import sys

import pointblank as pb

from polars_utils import utils
from polars_utils.validation import actions as vl
from polars_utils.validation.constants import GLOBAL_ACTIONS, GLOBAL_THRESHOLDS
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys
from utils.column_names.raw_data_files.ons_columns import (
    OnsPostcodeDirectoryColumns as ONS,
)

VALIDATED_COLUMNS = [
    ONS.postcode,
    Keys.import_date,
    ONS.cssr,
    ONS.region,
    ONS.sub_icb,
    ONS.icb,
    ONS.icb_region,
    ONS.lower_super_output_area_2021,
    ONS.middle_super_output_area_2021,
    ONS.rural_urban_indicator_2011,
]


def main(bucket_name: str, source_path: str, reports_path: str) -> None:
    """Validates raw ONS postcode directory data and produces a summary report plus failure outputs.

    Args:
        bucket_name (str): the bucket (name only) in which to source the dataset
            and output the report to (should correspond to workspace / feature
            branch name).
        source_path (str): the source dataset path to be validated.
        reports_path (str): the output path to write reports to.
    """
    source_df = utils.read_parquet(
        source=f"s3://{bucket_name}/{source_path}", selected_columns=VALIDATED_COLUMNS
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
            columns=VALIDATED_COLUMNS,
            brief="Key columns should contain no null values",
        )
        # index columns
        .rows_distinct(
            [ONS.postcode, Keys.import_date],
            brief=f"{ONS.postcode} and {Keys.import_date} together should be unique",
        ).interrogate()
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
