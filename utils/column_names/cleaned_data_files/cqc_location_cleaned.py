from dataclasses import dataclass

from utils.column_names.cleaned_data_files.ons_cleaned import (
    OnsCleanedColumns as ONSClean,
)
from utils.column_names.raw_data_files.cqc_location_api_columns import (
    NewCqcLocationApiColumns,
)


@dataclass
class CqcLocationCleanedColumns(NewCqcLocationApiColumns, ONSClean):
    all_contacts: str = "all_contacts"
    all_contacts_flat: str = "all_contacts_flat"
    contacts_full_name: str = "contacts_full_name"
    contacts_roles: str = "contacts_roles"
    contacts_exploded: str = "contacts_exploded"
    cqc_location_import_date: str = "cqc_location_import_date"
    cqc_sector: str = "cqc_sector"
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
    ons_contemporary_import_date: str = ONSClean.contemporary_ons_import_date
    ons_current_import_date: str = ONSClean.current_ons_import_date
    postcode_cleaned: str = NewCqcLocationApiColumns.postal_code + "_cleaned"
    postcode_truncated: str = postcode_cleaned + "_truncated"
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
    specialist_generalist_other_dementia: str = "specialist_generalist_other_dementia"
    specialist_generalist_other_lda: str = (
        "specialist_generalist_other_learning_disabilities"
    )
    specialist_generalist_other_mh: str = (
        "specialist_generalist_other_mental_health_conditions"
    )
