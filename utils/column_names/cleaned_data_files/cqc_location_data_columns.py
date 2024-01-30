from dataclasses import dataclass

from utils.column_names.raw_data_files.cqc_location_api_columns import (
    CqcLocationApiColumns,
)


@dataclass
class CqcLocationCleanedColumns(CqcLocationApiColumns):
    primary_service_type: str = "primary_service_type"
    sector: str = "sector"
