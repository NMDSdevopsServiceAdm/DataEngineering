import polars as pl

from polars_utils import utils
from utils.column_names.cleaned_data_files.cqc_location_cleaned import (
    CqcLocationCleanedColumns as CQCLClean,
)
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys


def allocate_import_dates(
    delta_flattened_s3_uri: str, full_flattened_s3_uri: str
) -> tuple[list[str], list[str]]:
    """
    Create a list of import_dates which need processing and a list of dates already processed.

    All dates in the delta dataset need to be converted to full files.
    `all_dates` represents all `import_dates` found in S3 for the delta dataset.
    `processed_dates` represents all `import_dates` found in S3 for the full dataset which have already been processed.
    `dates_to_process` are all import dates in `all_dates` which are not found in `processed_dates`.

    Args:
        delta_flattened_s3_uri (str): S3 URI for delta flattened files
        full_flattened_s3_uri (str): S3 URI for full flattened files

    Returns:
        tuple[list[str], list[str]]: List of import_dates to process and a list of dates already processed.
    """
    all_dates = utils.list_s3_parquet_import_dates(delta_flattened_s3_uri)
    processed_dates = utils.list_s3_parquet_import_dates(full_flattened_s3_uri)

    dates_to_process = [d for d in all_dates if d not in processed_dates]

    return dates_to_process, processed_dates


def load_latest_snapshot(
    destination_path: str, destination_import_dates: list[str]
) -> pl.LazyFrame | None:
    """
    Load the most recent snapshot from destination if it exists.

    Args:
        destination_path (str): S3 URI to read the full flattened CQC locations data from.
        destination_import_dates (list[str]): List of import_dates already present in the destination.

    Returns:
        pl.LazyFrame | None: A LazyFrame of latest snapshot or None if no snapshots exist).
    """
    if not destination_import_dates:
        return None

    latest_import_date = max(destination_import_dates)
    latest_lf = utils.scan_parquet(destination_path).filter(
        pl.col(Keys.import_date) == latest_import_date
    )
    return latest_lf


def create_full_snapshot(
    full_lf: pl.LazyFrame | None, delta_lf: pl.LazyFrame
) -> pl.LazyFrame:
    """
    Merge delta into full snapshot, keeping latest record per location_id.

    Args:
        full_lf (pl.LazyFrame | None): Existing full flattened LazyFrame or None if first snapshot.
        delta_lf (pl.LazyFrame): Delta flattened LazyFrame to merge.

    Returns:
        pl.LazyFrame: Merged LazyFrame (or just delta_lf if full_lf is None).
    """
    if full_lf is None:
        return delta_lf

    merged_lf = pl.concat([full_lf, delta_lf]).unique(
        subset=[CQCLClean.location_id], keep="last"
    )
    return merged_lf


def apply_partitions(lf: pl.LazyFrame, import_date: int | str) -> pl.LazyFrame:
    """
    Assign or replace partition columns for a given import_date (YYYYMMDD).

    Args:
        lf (pl.LazyFrame): LazyFrame to apply partitions.
        import_date (int | str): Import date in YYYYMMDD format (can be int or str).

    Returns:
        pl.LazyFrame: LazyFrame with partition columns applied.
    """
    date_str = str(import_date)

    return lf.with_columns(
        [
            pl.lit(date_str).cast(pl.Int32).alias(Keys.import_date),
            pl.lit(date_str[:4]).cast(pl.Int32).alias(Keys.year),
            pl.lit(date_str[4:6]).cast(pl.Int32).alias(Keys.month),
            pl.lit(date_str[6:8]).cast(pl.Int32).alias(Keys.day),
        ]
    )
