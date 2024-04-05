from dataclasses import dataclass

from utils.column_names.raw_data_files.ascwds_worker_columns import (
    AscwdsWorkerColumns,
)


@dataclass
class AscwdsWorkerCleanedColumns(AscwdsWorkerColumns):
    ascwds_worker_import_date: str = "ascwds_worker_import_date"
    main_job_role_labelled: str = "mainjrid_labels"
