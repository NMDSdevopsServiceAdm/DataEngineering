import polars as pl

from utils.column_names.ind_cqc_pipeline_columns import (
    ArchiveRunLogColumns as RunLogCols,
)

# The run log's file columns; the partition keys come from the folder names.
ARCHIVE_RUN_LOG_FILE_SCHEMA: dict[str, pl.DataType] = {
    RunLogCols.archive_date_time: pl.Datetime,
    RunLogCols.max_ascwds_workplace_import_date: pl.Date,
    RunLogCols.max_cqc_location_import_date: pl.Date,
    RunLogCols.max_cqc_pir_import_date: pl.Date,
    RunLogCols.max_ct_care_home_import_date: pl.Date,
    RunLogCols.max_ct_non_res_import_date: pl.Date,
    RunLogCols.max_current_ons_import_date: pl.Date,
    RunLogCols.approved: pl.Boolean,
    RunLogCols.commit_sha: pl.String,
    RunLogCols.tag: pl.String,
    RunLogCols.selected_for_publication: pl.Boolean,
    RunLogCols.reconciled: pl.Boolean,
    RunLogCols.reconciled_at: pl.Datetime,
    RunLogCols.locally_checked: pl.Boolean,
    RunLogCols.promoted: pl.Boolean,
    RunLogCols.promoted_at: pl.Datetime,
}
