from dataclasses import dataclass

from utils.column_names.raw_data_files.cqc_provider_api_columns import (
    CqcProviderApiColumns,
)


@dataclass
class CqcProviderCleanedColumns(CqcProviderApiColumns):
    sector: str = "sector"
