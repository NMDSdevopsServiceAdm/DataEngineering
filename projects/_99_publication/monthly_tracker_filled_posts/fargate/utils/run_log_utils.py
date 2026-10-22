import polars as pl

from polars_utils import utils
from utils.column_names.ind_cqc_pipeline_columns import (
    ArchiveDateRunNumberPartitionKeys as ArchiveKeys,
)
from utils.column_names.ind_cqc_pipeline_columns import (
    ArchiveRunLogColumns as RunLog,
)


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
