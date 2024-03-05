from dataclasses import dataclass

from utils.column_names.raw_data_files.cqc_location_api_columns import (
    CqcLocationApiColumns,
)
from utils.column_names.cleaned_data_files.cqc_provider_cleaned_values import (
    CqcProviderCleanedColumns as CQCPClean,
    CqcProviderCleanedValues as CQCPValues,
)
from utils.column_names.cleaned_data_files.ons_cleaned_values import (
    OnsCleanedColumns as ONSClean,
)


@dataclass
class CqcLocationCleanedColumns(CqcLocationApiColumns):
    import_date: str = "import_date"
    primary_service_type: str = "primary_service_type"
    cqc_sector: str = CQCPClean.cqc_sector
    provider_name: str = "provider_name"
    cqc_location_import_date: str = "cqc_location_import_date"
    ons_contemporary_import_date: str = ONSClean.ons_import_date
    ons_contemporary_geographies: str = ONSClean.contemporary
    ons_current_import_date: str = ONSClean.current_ons_import_date
    ons_current_geographies: str = ONSClean.current


@dataclass
class CqcLocationCleanedValues:
    care_home_with_nursing: str = "Care home with nursing"
    care_home_only: str = "Care home without nursing"
    non_residential: str = "non-residential"
    independent: str = CQCPValues.independent
    registered: str = "Registered"
    deregistered: str = "Deregistered"
    social_care_identifier: str = "Social Care Org"
