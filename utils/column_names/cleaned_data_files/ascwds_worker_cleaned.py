from dataclasses import dataclass

from utils.column_names.raw_data_files.ascwds_worker_columns import (
    AscwdsWorkerColumns,
)


@dataclass
class AscwdsWorkerCleanedColumns(AscwdsWorkerColumns):
    ascwds_worker_import_date: str = "ascwds_worker_import_date"
    main_job_role_clean: str = AscwdsWorkerColumns.main_job_role_id + "_clean"
    main_job_role_clean_labelled: str = main_job_role_clean + "_labels"
    ascwds_main_job_role_counts: str = "ascwds_main_job_role_counts"
