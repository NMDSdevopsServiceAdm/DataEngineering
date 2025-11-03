import polars as pl

from polars_utils import utils
from utils.column_names.cleaned_data_files.cqc_location_cleaned import (
    CqcLocationCleanedColumns as CQCLClean,
)
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys


def get_import_dates_to_process(
    delta_source_lf: pl.LazyFrame,
    existing_full_import_dates: list[str],
) -> list[str]:
    """
    Identify which import_dates exist in the source data but haven't been processed into the full file format.

    Args:
        delta_source_lf (pl.LazyFrame): LazyFrame of the delta flattened source data.
        existing_full_import_dates (list[str]): List of import_dates already present in the full flattened destination.

    Returns:
        list[str]: List of import_dates to process.
    """
    source_import_dates = (
        delta_source_lf.select(Keys.import_date)
        .unique()
        .sort(Keys.import_date)
        .collect()
        .to_series()
        .to_list()
    )

    import_dates_to_process = [
        d for d in source_import_dates if d not in existing_full_import_dates
    ]

    return import_dates_to_process


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
