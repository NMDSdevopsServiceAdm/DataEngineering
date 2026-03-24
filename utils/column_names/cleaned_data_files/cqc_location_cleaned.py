from dataclasses import dataclass

from utils.column_names.cleaned_data_files.ons_cleaned import (
    OnsCleanedColumns as ONSClean,
)
from utils.column_names.raw_data_files.cqc_location_api_columns import (
    NewCqcLocationApiColumns,
)


@dataclass
class CqcLocationCleanedColumns(NewCqcLocationApiColumns, ONSClean):
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
    primary_service_type_second_level: str = primary_service_type + "_second_level"
    provider_name: str = "provider_name"
    registered_manager_names: str = "registered_manager_names"
    regulated_activities_offered: str = "regulated_activities_offered"
    related_location: str = "related_location"
    relationships_types: str = "relationships_types"
    relationships_exploded: str = NewCqcLocationApiColumns.relationships + "_exploded"
    relationships_predecessors_only: str = (
        NewCqcLocationApiColumns.relationships + "_predecessors_only"
    )
    services_offered: str = "services_offered"
    specialisms_offered: str = "specialisms_offered"
    specialism_dementia: str = "specialism_dementia"
    specialism_learning_disabilities: str = "specialism_learning_disabilities"
    specialism_mental_health: str = "specialism_mental_health_conditions"


@dataclass
class CqcLocationCleanedNewValidationColumns:
    registered_manager_names_has_no_empty_or_null: str = (
        "registered_manager_names_has_no_empty_or_null"
    )
    regulated_activities_offered_has_no_empty_or_null: str = (
        "regulated_activities_offered_has_no_empty_or_null"
    )
    regulated_activities_offered_is_not_null: str = (
        "regulated_activities_offered_is_not_null"
    )
    services_offered_is_not_null: str = "services_offered_is_not_null"
    services_offered_has_no_empty_or_null: str = "services_offered_has_no_empty_or_null"
    specialisms_offered_is_not_null: str = "specialisms_offered_is_not_null"
    specialisms_offered_has_no_empty_or_null: str = (
        "specialisms_offered_has_no_empty_or_null"
    )
