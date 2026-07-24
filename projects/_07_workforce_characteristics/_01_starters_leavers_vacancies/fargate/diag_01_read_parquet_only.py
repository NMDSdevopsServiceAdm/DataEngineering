import sys

import projects._07_workforce_characteristics._01_starters_leavers_vacancies.fargate.utils.diag_helpers as diag
from polars_utils import utils


def main(bucket_name: str, source_path: str, reports_path: str) -> None:
    """Isolates the memory cost of utils.read_parquet()'s eager collect alone.

    Throwaway diagnostic for the ticket 1814 validate_00_prepare OOM - see the
    isolation plan, not part of the permanent pipeline.

    Args:
        bucket_name (str): the bucket containing the source dataset and to
            write diagnostic checkpoints to.
        source_path (str): the filepath of the dataset to read.
        reports_path (str): the filepath to write diagnostic checkpoints to.
    """
    diag.write_checkpoint(bucket_name, reports_path, "diag_01_before_read")

    source_df = utils.read_parquet(source=f"s3://{bucket_name}/{source_path}")

    diag.write_checkpoint(
        bucket_name,
        reports_path,
        "diag_01_after_read",
        row_count=source_df.height,
    )


if __name__ == "__main__":
    print(f"Diagnostic script called with parameters: {sys.argv}")

    args = utils.get_args(
        ("--bucket_name", "S3 bucket for source dataset and diagnostic checkpoints"),
        ("--source_path", "The filepath of the dataset to read"),
        ("--reports_path", "The filepath to write diagnostic checkpoints"),
    )

    main(args.bucket_name, args.source_path, args.reports_path)
    print("Diagnostic diag_01_read_parquet_only complete")
