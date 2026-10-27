import tempfile
from datetime import date
from pathlib import Path

import boto3
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

# The file name utils.sink_to_parquet writes, and update_run_log.py leaves behind.
RUN_LOG_FILE_NAME = "file.parquet"


def mark_run_log_flag(run_log_source: str, run_number: int, flag: str) -> None:
    """
    Sets a flag (and its timestamp) for one run in the S3 run log.

    Downloads the run's partition, updates it with scripts.update_run_log, and
    overwrites its file.parquet in one upload. Does nothing if the flag is
    already set. A rejected update's ValueError propagates unchanged.

    Raises:
        ValueError: if the partition holds anything other than file.parquet,
            which the update script always leaves. Nothing is written, as the
            task role can't delete a stale file.

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
            partition_dir / RUN_LOG_FILE_NAME
        )

        run_log_df = read_run_log(download_dir)
        if run_log_df[flag].item() is True:
            return
        month_start = date.fromisoformat(archive_date).replace(day=1)
        updated_df = set_run_flag(run_log_df, run_number, flag, month_start)
        write_run_partition(updated_df, run_number, output_dir)

        existing_keys = [
            obj["Key"]
            for obj in boto3.client("s3")
            .list_objects_v2(Bucket=bucket, Prefix=partition_prefix)
            .get("Contents", [])
        ]
        if existing_keys != [f"{partition_prefix}{RUN_LOG_FILE_NAME}"]:
            raise ValueError(
                f"Expected only {RUN_LOG_FILE_NAME} in s3://{bucket}/{partition_prefix}, "
                f"found {existing_keys}"
            )

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
