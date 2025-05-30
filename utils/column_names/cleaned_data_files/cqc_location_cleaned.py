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
    contacts_full_name: str = "contacts_full_name"
    contacts_roles: str = "contacts_roles"
    contacts_exploded: str = "contacts_exploded"
    cqc_location_import_date: str = "cqc_location_import_date"
    cqc_provider_import_date: str = CQCPClean.cqc_provider_import_date
    cqc_sector: str = CQCPClean.cqc_sector
    dormant_date: str = "dormant_date"
    earliest_import_date_per_dormancy_value: str = (
        "earliest_import_date_per_dormancy_value"
    )
    first_known_relationships: str = (
        "first_known_" + NewCqcLocationApiColumns.relationships
    )
    import_date: str = "import_date"
    imputed_gac_service_types: str = (
        "imputed_" + NewCqcLocationApiColumns.gac_service_types
    )
    imputed_registration_date: str = "imputed_registration_date"
    imputed_regulated_activities: str = (
        "imputed_" + NewCqcLocationApiColumns.regulated_activities
    )
    imputed_regulated_activities_exploded: str = "imputed_regulated_activities_exploded"
    imputed_relationships: str = "imputed_" + NewCqcLocationApiColumns.relationships
    imputed_specialisms: str = "imputed_" + NewCqcLocationApiColumns.specialisms
    last_dormant_date: str = "last_dormant_date"
    months_since_dormant: str = "months_since_dormant"
    ons_contemporary_import_date: str = ONSClean.contemporary_ons_import_date
    ons_current_import_date: str = ONSClean.current_ons_import_date
    primary_service_type: str = "primary_service_type"
    provider_name: str = "provider_name"
    registered_manager_names: str = "registered_manager_names"
    related_location: str = "related_location"
    relationships_exploded: str = NewCqcLocationApiColumns.relationships + "_exploded"
    relationships_predecessors_only: str = (
        NewCqcLocationApiColumns.relationships + "_predecessors_only"
    )
    services_offered: str = "services_offered"
    specialisms_offered: str = "specialisms_offered"
    time_registered: str = "time_registered"
    time_since_dormant: str = "time_since_dormant"
