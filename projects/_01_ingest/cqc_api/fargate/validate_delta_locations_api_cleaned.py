"""
Note: compare_path isn't used in this script, but I've left it in commented out so
we can use it later when converting other jobs to pointblank.
At that point, we can remove the commented out code from this job.
"""

import sys

import pointblank as pb
import polars as pl

from polars_utils import utils
from polars_utils.expressions import has_value, str_length_cols
from polars_utils.logger import get_logger
from polars_utils.raw_data_adjustments import is_valid_location
from polars_utils.validation import actions as vl
from polars_utils.validation.constants import GLOBAL_ACTIONS, GLOBAL_THRESHOLDS
from utils.column_names.cleaned_data_files.cqc_location_cleaned import (
    CqcLocationCleanedColumns as CQCLClean,
)
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys
from utils.column_names.raw_data_files.cqc_location_api_columns import (
    NewCqcLocationApiColumns as CQCL,
)
from utils.column_names.validation_table_columns import Validation
from utils.column_values.categorical_column_values import LocationType, Services
from utils.column_values.categorical_columns_by_dataset import (
    LocationsApiCleanedCategoricalValues as CatValues,
)

compare_columns_to_import = [
    Keys.import_date,
    CQCL.location_id,
    CQCL.provider_id,
    CQCL.type,
    CQCL.registration_status,
    CQCL.gac_service_types,
    CQCL.regulated_activities,
]


logger = get_logger(__name__)


def main(bucket_name: str, source_path: str, reports_path: str) -> None:
    # def main(bucket_name: str, source_path: str, reports_path: str, compare_path: str) -> None:

    """Validates a dataset according to a set of provided rules and produces a summary report as well as failure outputs.

    Args:
        bucket_name (str): the bucket (name only) in which to source the dataset and output the report to
            - shoud correspond to workspace / feature branch name
        source_path (str): the source dataset path to be validated
        reports_path (str): the output path to write reports to
    """
    source_df = utils.read_parquet(
        f"s3://{bucket_name}/{source_path}", exclude_complex_types=True
    ).with_columns(
        str_length_cols([CQCL.location_id, CQCL.provider_id]),
    )
    # compare_df = utils.read_parquet(
    #     f"s3://{bucket_name}/{compare_path}",
    #     selected_columns=compare_columns_to_import,
    # )

    validation = (
        pb.Validate(
            data=source_df,
            label=f"Validation of {source_path}",
            thresholds=GLOBAL_THRESHOLDS,
            brief=True,
            actions=GLOBAL_ACTIONS,
        )
        # dataset size
        # .row_count_match(expected_size(compare_df))
        # complete columns
        .col_vals_not_null(
            [
                CQCLClean.location_id,
                CQCLClean.cqc_location_import_date,
                CQCLClean.provider_id,
                CQCLClean.cqc_sector,
                CQCLClean.registration_status,
                CQCLClean.imputed_registration_date,
                CQCLClean.name,
            ]
        )
        # index columns
        .rows_distinct(
            [
                CQCLClean.location_id,
                CQCLClean.cqc_location_import_date,
            ],
        )
        # between (inclusive)
        .col_vals_between(CQCLClean.number_of_beds, 0, 500, na_pass=True)
        .col_vals_between(Validation.location_id_length, 3, 14)
        .col_vals_between(Validation.provider_id_length, 3, 14)
        # categorical
        .col_vals_in_set(
            CQCLClean.cqc_sector, CatValues.sector_column_values.categorical_values
        )
        .col_vals_in_set(
            CQCLClean.registration_status,
            CatValues.registration_status_column_values.categorical_values,
        )
        .col_vals_in_set(
            CQCLClean.dormancy,
            # na_pass is not an optional parameter to .col_vals_in_set
            # extending list to allow for null values as not included
            # in categorical values
            [*CatValues.dormancy_column_values.categorical_values, None],
        )
        .col_vals_in_set(
            CQCLClean.related_location,
            CatValues.related_location_column_values.categorical_values,
        )
        # distinct values
        .specially(
            vl.is_unique_count_equal(
                CQCLClean.cqc_sector,
                CatValues.sector_column_values.count_of_categorical_values,
            ),
            brief=f"{CQCLClean.cqc_sector} needs to be one of {CatValues.sector_column_values.categorical_values}",
        )
        .specially(
            vl.is_unique_count_equal(
                CQCLClean.registration_status,
                CatValues.registration_status_column_values.count_of_categorical_values,
            ),
            brief=f"{CQCLClean.registration_status} needs to be one of {CatValues.registration_status_column_values.categorical_values}",
        )
        .specially(
            vl.is_unique_count_equal(
                CQCLClean.dormancy,
                CatValues.dormancy_column_values.count_of_categorical_values,
            ),
            brief=f"{CQCLClean.dormancy} needs to be null, or one of {CatValues.dormancy_column_values.categorical_values}",
        )
        .specially(
            vl.is_unique_count_equal(
                CQCLClean.related_location,
                CatValues.related_location_column_values.count_of_categorical_values,
            ),
            brief=f"{CQCLClean.related_location} needs to be one of {CatValues.related_location_column_values.categorical_values}",
        )
        .interrogate()
    )
    vl.write_reports(validation, bucket_name, reports_path)


# def expected_size(df: pl.DataFrame) -> int:
#     gac_services = pl.col(CQCL.gac_service_types)
#     cleaned_df = df.with_columns(
#         # nullify empty lists to avoid index out of bounds error
#         pl.when(gac_services.list.len() > 0).then(gac_services),
#     ).filter(
#         # TODO: remove regulated_activities
#         has_value(df, CQCL.regulated_activities, CQCL.location_id),
#         has_value(df, CQCL.provider_id, CQCL.location_id),
#         pl.col(CQCL.type) == LocationType.social_care_identifier,
#         is_valid_location(),
#         ~(
#             (gac_services.list.len() == 1)
#             & (gac_services.is_not_null())
#             & (
#                 gac_services.list[0].struct.field(CQCL.description)
#                 == Services.specialist_college_service
#             )
#         ),
#     )
#     logger.info(f"Expected size {cleaned_df.height}")
#     return cleaned_df.height


if __name__ == "__main__":
    logger.info(f"Validation script called with parameters: {sys.argv}")

    args = utils.get_args(
        ("--bucket_name", "S3 bucket for source dataset and validation report"),
        ("--source_path", "The filepath of the dataset to validate"),
        ("--reports_path", "The filepath to output reports"),
        # (
        #     "--compare_path",
        #     "The filepath to a dataset to compare against for expected size",
        # ),
    )
    logger.info(f"Starting validation for {args.source_path}")

    # main(args.bucket_name, args.source_path, args.reports_path, args.compare_path)
    main(args.bucket_name, args.source_path, args.reports_path)
    logger.info(f"Validation of {args.source_path} complete")
