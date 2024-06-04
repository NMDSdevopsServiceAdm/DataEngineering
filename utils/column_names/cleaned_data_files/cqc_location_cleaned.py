from dataclasses import dataclass

from utils.column_names.raw_data_files.cqc_location_api_columns import (
    NewCqcLocationApiColumns,
)
from utils.column_names.cleaned_data_files.cqc_provider_cleaned import (
    CqcProviderCleanedColumns as CQCPClean,
)
from utils.column_names.cleaned_data_files.ons_cleaned import (
    OnsCleanedColumns as ONSClean,
)


@dataclass
class CqcLocationCleanedColumns(NewCqcLocationApiColumns, ONSClean):
    import_date: str = "import_date"
    services_offered: str = "services_offered"
    primary_service_type: str = "primary_service_type"
    cqc_sector: str = CQCPClean.cqc_sector
    provider_name: str = "provider_name"
    cqc_location_import_date: str = "cqc_location_import_date"
    cqc_provider_import_date: str = CQCPClean.cqc_provider_import_date
    ons_contemporary_import_date: str = ONSClean.contemporary_ons_import_date
    ons_current_import_date: str = ONSClean.current_ons_import_date
