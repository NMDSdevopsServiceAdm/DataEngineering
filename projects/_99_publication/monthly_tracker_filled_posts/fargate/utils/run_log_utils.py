import tempfile
from datetime import date
from pathlib import Path

import polars as pl

from polars_utils import utils
from scripts.update_run_log import read_run_log, set_run_flag, write_run_partition
from utils import file_utils
from utils.column_names.ind_cqc_pipeline_columns import (
    ArchiveDateRunNumberPartitionKeys as ArchiveKeys,
)
from utils.column_names.ind_cqc_pipeline_columns import (
    ArchiveRunLogColumns as RunLog,
)


def mark_run_log_flag(run_log_source: str, run_number: int, flag: str) -> None:
    """
    Sets a flag (and any companion timestamp) for one run directly in the S3 run
    log, reusing update_run_log.py's own local-file flag logic rather than
    reimplementing it.

    Downloads only the target run's partition, delegates validation and the
    flag/timestamp update to scripts.update_run_log, then re-uploads --
    clearing the destination partition first so no stale file is left alongside
    the new one.

    Propagates update_run_log.set_run_flag's ValueError unchanged if it rejects
    the update (e.g. the run isn't eligible yet, or the flag is already set).

    Args:
        run_log_source (str): S3 directory of the archive run log, partitioned by
            archive_date and run_number
        run_number (int): the run to update
        flag (str): the flag column to set, one of update_run_log.SUPPORTED_FLAGS
    """
    archive_date = file_utils.list_archive_runs(run_log_source)[run_number]
    bucket, root_prefix = file_utils.split_s3_uri(run_log_source.rstrip("/") + "/")
    partition_prefix = (
        f"{root_prefix}{ArchiveKeys.archive_date}={archive_date}/"
        f"{ArchiveKeys.run_number}={run_number}/"
    )
    partition_uri = file_utils.construct_s3_uri(bucket, partition_prefix)

    with tempfile.TemporaryDirectory() as temp_dir:
        download_dir = Path(temp_dir) / "downloaded"
        output_dir = Path(temp_dir) / "updated"
        partition_dir = (
            download_dir
            / f"{ArchiveKeys.archive_date}={archive_date}"
            / f"{ArchiveKeys.run_number}={run_number}"
        )
        partition_dir.mkdir(parents=True)
        utils.scan_parquet(partition_uri).collect().write_parquet(
            partition_dir / "file.parquet"
        )

        run_log_df = read_run_log(download_dir)
        month_start = date.fromisoformat(archive_date).replace(day=1)
        updated_df = set_run_flag(run_log_df, run_number, flag, month_start)
        write_run_partition(updated_df, run_number, output_dir)

        file_utils.empty_s3_folder(bucket, partition_prefix)
        updated_file = next(output_dir.rglob("*.parquet"))
        utils.sink_to_parquet(pl.scan_parquet(updated_file), partition_uri)


def get_previous_published_run_number(
    run_log_source: str, run_number: int
) -> int | None:
    """
    Finds the highest published run number below the given run.

    A run is published when it is selected for publication in the run log.

    Args:
        run_log_source (str): S3 directory of the archive run log, partitioned by
            archive_date and run_number
        run_number (int): the current run number

    Returns:
        int | None: the highest published run number below run_number, or None
            when no earlier run has been published
    """
    return (
        utils.scan_parquet(
            run_log_source,
            selected_columns=[ArchiveKeys.run_number, RunLog.selected_for_publication],
        )
        .filter(
            pl.col(RunLog.selected_for_publication)
            & (pl.col(ArchiveKeys.run_number) < run_number)
        )
        .select(pl.col(ArchiveKeys.run_number).max())
        .collect()
        .item()
    )
