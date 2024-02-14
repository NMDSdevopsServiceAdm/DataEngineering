from dataclasses import dataclass

from utils.column_names.raw_data_files.cqc_provider_api_columns import (
    CqcProviderApiColumns,
)


@dataclass
class CqcProviderCleanedColumns(CqcProviderApiColumns):
    cqc_sector: str = "cqc_sector"


@dataclass
class CqcProviderCleanedValues:
    local_authority: str = "Local authority"
    independent: str = "Independent"
