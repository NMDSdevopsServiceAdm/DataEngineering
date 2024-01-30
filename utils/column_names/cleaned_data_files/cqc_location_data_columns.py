from dataclasses import dataclass

from utils.column_names.raw_data_files.cqc_location_api_columns import (
    CqcLocationApiColumns,
)
from utils.column_names.cleaned_data_files.cqc_provider_data_columns import (
    CqcProviderCleanedColumns as CQCPClean,
)


@dataclass
class CqcLocationCleanedColumns(CqcLocationApiColumns):
    primary_service_type: str = "primary_service_type"
    sector: str = CQCPClean.sector
