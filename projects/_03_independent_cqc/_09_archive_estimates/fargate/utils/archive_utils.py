import re
from datetime import datetime

import boto3
import polars as pl

from utils.column_names.ind_cqc_pipeline_columns import (
    ArchivePartitionKeys as ArchiveKeys,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.file_utils import split_s3_uri

most_recent_annual_estimate_date: str = "most_recent_annual_estimate_date"


def select_import_dates_to_archive(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Filters LazyFrame to only include the most recent monthly estimates, plus historical annual estimates.

    Args:
        lf (pl.LazyFrame): A LazyFrame to archive.

    Returns:
        pl.LazyFrame: A LazyFrame with the most recent monthly estimates, plus historical annual estimates.
    """

    lf = add_latest_annual_estimate_date(lf)

    import_date_col = pl.col(IndCQC.cqc_location_import_date)
    annual_estimate_date_col = pl.col(most_recent_annual_estimate_date)

    import_on_or_after_annual_estimate = import_date_col >= annual_estimate_date_col
    import_before_annual_estimate = import_date_col < annual_estimate_date_col
    import_month_equals_annual_estimate_month = (
        import_date_col.dt.month() == annual_estimate_date_col.dt.month()
    )

    lf = lf.filter(
        import_on_or_after_annual_estimate
        | (import_before_annual_estimate & import_month_equals_annual_estimate_month)
    )

    return lf.drop(most_recent_annual_estimate_date)


def add_latest_annual_estimate_date(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Adds a date column with the value 1st April and the year of latest annual estimates publication. For example,
    for estimates published for 2024/25 this will be 2025-04-01.

    Args:
        lf (pl.LazyFrame): A LazyFrame with the column cqc_location_import_date.

    Returns:
        pl.LazyFrame: The input LazyFrame with column for most recent annual estimate date.
    """
    max_date = pl.col(IndCQC.cqc_location_import_date).max()

    april = 4

    lf = lf.with_columns(
        pl.when(max_date.dt.month() < april)
        .then(pl.date(max_date.dt.year() - 1, april, 1))
        .otherwise(pl.date(max_date.dt.year(), april, 1))
        .alias(most_recent_annual_estimate_date)
    )

    return lf


def create_archive_date_partition_columns(
    lf: pl.LazyFrame, date_time: datetime
) -> pl.LazyFrame:
    """
    Adds columns for archive day, month, year and timestamp using the given datetime.

    Args:
        lf(pl.LazyFrame): A LazyFrame with a data column.
        date_time(datetime): A date time to be used to construct the archive partition columns.

    Returns:
        pl.LazyFrame: A LazyFrame with archive day, month, and year columns added.
    """
    day = date_time.strftime("%d")
    month = date_time.strftime("%m")
    year = date_time.strftime("%Y")
    timestamp = date_time.strftime("%Y-%m-%d %H:%M")
    lf = lf.with_columns(
        (pl.lit(day).alias(ArchiveKeys.archive_day)),
        (pl.lit(month).alias(ArchiveKeys.archive_month)),
        (pl.lit(year).alias(ArchiveKeys.archive_year)),
        (pl.lit(timestamp).alias(ArchiveKeys.archive_timestamp)),
    )

    return lf


def get_run_number(s3_roots: list[str]) -> int:
    """
    Finds the highest existing run_number already archived under each of the given
    S3 roots, across all archive_dates, and confirms they agree.

    Scans all objects under each s3_root and extracts the run_number values from
    keys structured like:
        bucket/domain=ind_cqc_filled_posts/dataset=ind_cqc_09_archived_monthly_job_role_estimates/

    run_number is a single counter shared across every archive_date, not scoped
    to a particular one, so this always looks at the full history under s3_root.

    Args:
        s3_roots (list[str]): S3 directories a set of related archive outputs are
            written to (e.g. an archive job's estimates, metadata, and geography
            destinations), which are expected to always share the same run_number.

    Returns:
        int: The highest existing run_number shared by all given s3_roots, or `0`
            if none of them have any runs yet.

    Raises:
        ValueError: If the s3_roots disagree on the highest existing run_number.
    """
    s3_client = boto3.client("s3")
    paginator = s3_client.get_paginator("list_objects_v2")

    run_number_by_root: dict[str, int] = {}
    for s3_root in s3_roots:
        bucket, prefix = split_s3_uri(s3_root.rstrip("/") + "/")
        pages = paginator.paginate(Bucket=bucket, Prefix=prefix)

        run_numbers = [
            int(match.group(1))
            for page in pages
            for obj in page.get("Contents", [])
            if (match := re.search(r"run_number=(\d+)", obj["Key"]))
        ]
        run_number_by_root[s3_root] = max(run_numbers, default=0)

    distinct_run_numbers = set(run_number_by_root.values())
    if len(distinct_run_numbers) > 1:
        raise ValueError(
            f"run_number has diverged between archive destinations: {run_number_by_root}"
        )

    return distinct_run_numbers.pop()
