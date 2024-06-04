from dataclasses import dataclass

from utils.column_names.raw_data_files.cqc_provider_api_columns import (
    CqcProviderApiColumns,
)


@dataclass
class CqcProviderCleanedColumns(CqcProviderApiColumns):
    cqc_sector: str = "cqc_sector"
    cqc_provider_import_date: str = "cqc_provider_import_date"
