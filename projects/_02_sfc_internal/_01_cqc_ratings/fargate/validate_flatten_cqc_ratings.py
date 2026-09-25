import sys

import pointblank as pb

from polars_utils import utils
from polars_utils.validation import actions as vl
from polars_utils.validation.constants import GLOBAL_ACTIONS, GLOBAL_THRESHOLDS
from utils.column_names.cqc_ratings_columns import CQCRatingsColumns as CQCRatings
from utils.column_names.raw_data_files.cqc_location_api_columns import (
    NewCqcLocationApiColumns as CQCL,
)
from utils.column_values.categorical_column_values import (
    CQCCurrentOrHistoricValues,
    CQCRatingsValues,
    RegistrationStatus,
)

rating_columns = [
    CQCRatings.overall_rating,
    CQCRatings.safe_rating,
    CQCRatings.well_led_rating,
    CQCRatings.caring_rating,
    CQCRatings.responsive_rating,
    CQCRatings.effective_rating,
]

rating_value_columns = [
    CQCRatings.safe_rating_value,
    CQCRatings.well_led_rating_value,
    CQCRatings.caring_rating_value,
    CQCRatings.responsive_rating_value,
    CQCRatings.effective_rating_value,
]

grain_columns = [
    CQCL.location_id,
    CQCRatings.date,
    CQCL.assessment_plan_id,
    CQCL.name,
    CQCL.source_path,
    CQCL.dataset,
    CQCRatings.current_or_historic,
    # prepare_historic_ratings' own pivot index includes overall_rating,
    # since two historic reports can share a date but differ in this value.
    CQCRatings.overall_rating,
]

rating_values = CQCRatingsValues(CQCRatings.overall_rating, contains_null_values=True)


def main(bucket_name: str, source_path: str, reports_path: str) -> None:
    """Validates the flattened CQC ratings dataset and produces a summary report.

    Args:
        bucket_name (str): the bucket (name only) in which to source the dataset and
            output the report to - should correspond to workspace / feature branch name
        source_path (str): the source dataset path to be validated
        reports_path (str): the output path to write reports to
    """
    source_df = utils.read_parquet(f"s3://{bucket_name}/{source_path}")

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
                CQCL.location_id,
                CQCL.registration_status,
                CQCRatings.date,
                CQCL.dataset,
                CQCRatings.current_or_historic,
                CQCRatings.latest_rating_flag,
                CQCRatings.total_rating_value,
            ]
        )
        # index columns
        .rows_distinct(grain_columns)
        # categorical
        .col_vals_in_set(
            CQCL.registration_status,
            RegistrationStatus(CQCL.registration_status).categorical_values,
        )
        .col_vals_in_set(
            CQCRatings.current_or_historic,
            CQCCurrentOrHistoricValues(
                CQCRatings.current_or_historic
            ).categorical_values,
        )
        .col_vals_in_set(
            CQCL.dataset,
            ["Pre SAF", "SAF"],
        )
        .col_vals_in_set(
            rating_columns,
            [*rating_values.categorical_values, None],
        )
        # between (inclusive)
        .col_vals_between(CQCRatings.overall_rating_value, 0, 4)
        .col_vals_between(rating_value_columns, 0, 4)
        .col_vals_between(CQCRatings.total_rating_value, 0, 20)
        .col_vals_in_set(CQCRatings.latest_rating_flag, [0, 1])
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
