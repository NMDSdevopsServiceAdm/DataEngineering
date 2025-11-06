import sys

import pointblank as pb

from polars_utils import utils
from polars_utils.logger import get_logger
from polars_utils.validation import actions as vl
from polars_utils.validation.constants import GLOBAL_ACTIONS, GLOBAL_THRESHOLDS
from utils.column_names.cleaned_data_files.cqc_location_cleaned import (
    CqcLocationCleanedColumns as CQCLClean,
)
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys

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
                CQCLClean.location_id,
                Keys.import_date,
                CQCLClean.cqc_location_import_date,
                CQCLClean.imputed_registration_date,
                CQCLClean.registration_status,
                CQCLClean.provider_id,
                CQCLClean.services_offered,
                CQCLClean.specialisms_offered,
                CQCLClean.regulated_activities_offered,
                CQCLClean.registered_manager_names,
                CQCLClean.primary_service_type,
                CQCLClean.care_home,
                CQCLClean.cqc_sector,
                CQCLClean.related_location,
                CQCLClean.specialism_dementia,
                CQCLClean.specialism_learning_disabilities,
                CQCLClean.specialism_mental_health,
                # Add geography columns here after PR for branch 1117 is merged
            ]
        )
        # index columns
        .rows_distinct([CQCLClean.location_id, Keys.import_date]).interrogate()
        # social care only
        # registered locations only = Registered
        # CQCLClean.primary_service_type = CHWN COH non-res
        # CQCLClean.care_home = Y/N
        # no specialist colleges
        # CQCLClean.cqc_sector = LA/IND
        # CQCLClean.related_location = Y/N
        # CQCLClean.specialism_dementia = spec/gen/other
        # CQCLClean.specialism_learning_disabilities = spec/gen/other
        # CQCLClean.specialism_mental_health = spec/gen/other
    )
    vl.write_reports(validation, bucket_name, reports_path)


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
