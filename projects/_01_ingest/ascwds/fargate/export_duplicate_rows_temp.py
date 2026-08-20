"""THROWAWAY data-check export for ticket 1906.

Not part of the real pipeline - never wired into an auto-triggered Step
Function, never merged to main. Delete this file, its terraform/Step
Function wiring, and the temporary S3 output once the data check concludes.

Exports the original (pre-null), full-width rows for every establishment_id/
ascwds_workplace_import_date pair the new duplicate-detection filter flags,
so they can be reviewed in Athena to confirm they look like genuine
duplicate submissions.
"""

from polars_utils import cleaning_utils as cUtils
from polars_utils import utils
from projects._01_ingest.ascwds.fargate.utils import clean_workplace_utils as wUtils
from utils.column_names.cleaned_data_files.ascwds_workplace_cleaned import (
    AscwdsWorkplaceCleanedColumns as AWPClean,
)


def main(workplace_source: str, duplicate_rows_temp_destination: str) -> None:
    """Exports the original content of every duplicate-flagged row - see module docstring.

    Args:
        workplace_source (str): path to the raw ASC-WDS workplace data
        duplicate_rows_temp_destination (str): temporary destination for the
            flagged rows' original content
    """
    combined_schema = utils.discover_combined_schema(workplace_source)
    combined_lf = utils.scan_parquet(workplace_source, schema=combined_schema)

    # Same call as the real pipeline - reuses the already-fixed function/engine choice.
    duplicate_keys = (
        wUtils.find_duplicate_workplace_submissions(combined_lf)
        .collect(engine="streaming")
        .lazy()
    )

    combined_lf = cUtils.column_to_date(
        combined_lf, AWPClean.import_date, AWPClean.ascwds_workplace_import_date
    )
    flagged_rows_lf = combined_lf.join(
        duplicate_keys,
        on=[AWPClean.establishment_id, AWPClean.ascwds_workplace_import_date],
        how="semi",
    )

    utils.sink_to_parquet(flagged_rows_lf, output_path=duplicate_rows_temp_destination)


if __name__ == "__main__":
    args = utils.get_args(
        (
            "--workplace_source",
            "Source s3 directory for raw ASC-WDS workplace data",
        ),
        (
            "--duplicate_rows_temp_destination",
            "Temporary destination for the duplicate-flagged rows' original content",
        ),
    )
    main(
        workplace_source=args.workplace_source,
        duplicate_rows_temp_destination=args.duplicate_rows_temp_destination,
    )
