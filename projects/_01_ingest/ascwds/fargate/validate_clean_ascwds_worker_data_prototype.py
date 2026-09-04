"""THROWAWAY diagnostic prototype for ticket 2005 - not meant to reach main.

Re-runs ticket 1998's RunDiagnostics measurement against the real, permanent
`validate_clean_ascwds_worker_data.py` (same check chain, copied here rather
than imported so it can be wrapped in checkpoints without touching the real
job), since 1998's ~27% headroom finding was flagged with a library-version
confound and its own recommendation to re-measure whatever actually gets
built.

Delete this file (and its throwaway Step Function,
`Diagnose-ASCWDS-Worker-Validate.json`) once the investigation concludes.
"""

import sys

import pointblank as pb

from polars_utils import utils
from polars_utils.column_types import CategoricalColumnTypes
from polars_utils.run_diagnostics import RunDiagnostics
from polars_utils.validation import actions as vl
from polars_utils.validation.constants import GLOBAL_ACTIONS, GLOBAL_THRESHOLDS
from utils.column_names.cleaned_data_files.ascwds_worker_cleaned import (
    AscwdsWorkerCleanedColumns as ASCWKClean,
)
from utils.column_values.categorical_columns_by_dataset import (
    ASCWDSWorkerCleanedCategoricalValues as CatValues,
)

EXPECTED_SCHEMA = pb.Schema(
    {
        ASCWKClean.location_id: "String",
        ASCWKClean.establishment_id: "String",
        ASCWKClean.worker_id: "String",
        ASCWKClean.main_job_role_id: "String",
        ASCWKClean.ascwds_worker_import_date: "Date",
        ASCWKClean.main_job_role_clean: str(
            CategoricalColumnTypes.MainJobRoleIdCatType
        ),
        ASCWKClean.main_job_role_clean_labelled: str(
            CategoricalColumnTypes.JobRoleCatType
        ),
    }
)


def main(bucket_name: str, source_path: str, reports_path: str) -> None:
    """Runs the real worker validation rules, instrumented with
    RunDiagnostics to re-measure where memory peaks occur.

    Args:
        bucket_name (str): the bucket (name only) to source the dataset from
            and write the report/diagnostics to.
        source_path (str): the source dataset path to be validated.
        reports_path (str): the output path to write reports to. Must be a
            diagnostic-only path, distinct from the real validation report
            path, so a throwaway run can never clobber real output.
    """
    diagnostics = RunDiagnostics(
        "validate_clean_ascwds_worker_data_prototype", bucket_name
    ).start()
    print(f"Run diagnostics: s3://{diagnostics.bucket}/{diagnostics.prefix}")

    try:
        source_df = utils.read_parquet(source=f"s3://{bucket_name}/{source_path}")
        diagnostics.checkpoint("after_read")

        validation = pb.Validate(
            data=source_df,
            label=f"Validation of {source_path}",
            thresholds=GLOBAL_THRESHOLDS,
            brief=True,
            actions=GLOBAL_ACTIONS,
        )
        validation = (
            validation.col_schema_match(
                schema=EXPECTED_SCHEMA,
                brief="Dataset should match the expected schema",
            )
            .rows_distinct([ASCWKClean.worker_id, ASCWKClean.ascwds_worker_import_date])
            .col_vals_not_null(
                columns=[
                    ASCWKClean.establishment_id,
                    ASCWKClean.worker_id,
                    ASCWKClean.main_job_role_clean,
                    ASCWKClean.main_job_role_clean_labelled,
                    ASCWKClean.ascwds_worker_import_date,
                ],
                brief="Key columns should contain no null values",
            )
            .col_vals_in_set(
                ASCWKClean.main_job_role_clean,
                CatValues.main_job_role_id_column_values.categorical_values,
            )
            .col_vals_in_set(
                ASCWKClean.main_job_role_clean_labelled,
                CatValues.main_job_role_labels_column_values.categorical_values,
            )
            .specially(
                vl.is_unique_count_equal(
                    ASCWKClean.main_job_role_clean,
                    CatValues.main_job_role_id_column_values.count_of_categorical_values,
                ),
                brief=f"{ASCWKClean.main_job_role_clean} should have exactly {CatValues.main_job_role_id_column_values.count_of_categorical_values} distinct values",
            )
            .specially(
                vl.is_unique_count_equal(
                    ASCWKClean.main_job_role_clean_labelled,
                    CatValues.main_job_role_labels_column_values.count_of_categorical_values,
                ),
                brief=f"{ASCWKClean.main_job_role_clean_labelled} should have exactly {CatValues.main_job_role_labels_column_values.count_of_categorical_values} distinct values",
            )
        )

        diagnostics.checkpoint("before_interrogate")
        validation = validation.interrogate()
        diagnostics.checkpoint("after_interrogate")

        vl.write_reports(validation, bucket_name, reports_path)
        diagnostics.checkpoint("after_write_reports")
    finally:
        diagnostics.stop()


if __name__ == "__main__":
    print(f"Diagnostic validation script called with parameters: {sys.argv}")

    args = utils.get_args(
        ("--bucket_name", "S3 bucket for source dataset and validation report"),
        ("--source_path", "The filepath of the dataset to validate"),
        ("--reports_path", "The filepath to output reports"),
    )
    print(f"Starting diagnostic validation for {args.source_path}")

    main(args.bucket_name, args.source_path, args.reports_path)
    print(f"Diagnostic validation of {args.source_path} complete")
