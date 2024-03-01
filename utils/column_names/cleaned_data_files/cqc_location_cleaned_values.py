from dataclasses import dataclass

from utils.column_names.raw_data_files.cqc_location_api_columns import (
    CqcLocationApiColumns,
)
from utils.column_names.cleaned_data_files.cqc_provider_cleaned_values import (
    CqcProviderCleanedColumns as CQCPClean,
)
from utils.column_names.raw_data_files.ons_columns import (
    OnsPostcodeDirectoryColumns as ONS,
)


@dataclass
class CqcLocationCleanedColumns(CqcLocationApiColumns):
    import_date: str = "import_date"
    primary_service_type: str = "primary_service_type"
    sector: str = CQCPClean.cqc_sector
    provider_name: str = "provider_name"
    cqc_location_import_date: str = "cqc_location_import_date"
    current_cssr: str = "current_" + ONS.cssr
    current_region: str = "current_" + ONS.region
    current_icb: str = "current_" + ONS.icb
    current_rural_urban_indicator_2011: str = (
        "current_" + ONS.rural_urban_indicator_2011
    )
    ons_import_date: str = "ons_postcode_import_date"
    contemporary_cssr: str = ONS.cssr
    contemporary_region: str = ONS.region
    contemporary_icb: str = ONS.icb
    contemporary_rural_urban_indicator_2011: str = ONS.rural_urban_indicator_2011


@dataclass
class CqcLocationCleanedValues:
    care_home_with_nursing: str = "Care home with nursing"
    care_home_only: str = "Care home without nursing"
    non_residential: str = "non-residential"
