from dataclasses import dataclass

from utils.column_names.cleaned_data_files.cqc_location_cleaned import (
    CqcLocationCleanedColumns as CQCLClean,
)
from utils.column_values.categorical_columns_by_dataset import (
    LocationsApiCleanedCategoricalValues as CatValues,
)
from utils.column_names.validation_table_columns import (
    Validation,
)
from utils.validation.validation_rule_custom_type import CustomValidationRules
from utils.validation.validation_rule_names import RuleNames as RuleName


@dataclass
class LocationsAPICleanedValidationRules:
    rules_to_check = {
        RuleName.size_of_dataset: None,
        RuleName.complete_columns: [
            CQCLClean.location_id,
            CQCLClean.cqc_provider_import_date,
            CQCLClean.cqc_location_import_date,
            CQCLClean.care_home,
            CQCLClean.provider_id,
            CQCLClean.cqc_sector,
            CQCLClean.registration_status,
            CQCLClean.imputed_registration_date,
            CQCLClean.primary_service_type,
            CQCLClean.name,
            CQCLClean.provider_name,
            CQCLClean.contemporary_ons_import_date,
            CQCLClean.contemporary_cssr,
            CQCLClean.contemporary_region,
            CQCLClean.current_ons_import_date,
            CQCLClean.current_cssr,
            CQCLClean.current_region,
            CQCLClean.current_rural_urban_ind_11,
        ],
        RuleName.index_columns: [
            CQCLClean.location_id,
            CQCLClean.cqc_location_import_date,
        ],
        RuleName.min_values: {
            CQCLClean.number_of_beds: 0,
            CQCLClean.time_registered: 1,
            Validation.location_id_length: 3,
            Validation.provider_id_length: 3,
        },
        RuleName.max_values: {
            CQCLClean.number_of_beds: 500,
            Validation.location_id_length: 14,
            Validation.provider_id_length: 14,
        },
        RuleName.categorical_values_in_columns: {
            CQCLClean.care_home: CatValues.care_home_column_values.categorical_values,
            CQCLClean.cqc_sector: CatValues.sector_column_values.categorical_values,
            CQCLClean.registration_status: CatValues.registration_status_column_values.categorical_values,
            CQCLClean.dormancy: CatValues.dormancy_column_values.categorical_values,
            CQCLClean.primary_service_type: CatValues.primary_service_type_column_values.categorical_values,
            CQCLClean.contemporary_cssr: CatValues.contemporary_cssr_column_values.categorical_values,
            CQCLClean.contemporary_region: CatValues.contemporary_region_column_values.categorical_values,
            CQCLClean.current_cssr: CatValues.current_cssr_column_values.categorical_values,
            CQCLClean.current_region: CatValues.current_region_column_values.categorical_values,
            CQCLClean.current_rural_urban_ind_11: CatValues.current_rui_column_values.categorical_values,
            CQCLClean.related_location: CatValues.related_location_column_values.categorical_values,
        },
        RuleName.distinct_values: {
            CQCLClean.care_home: CatValues.care_home_column_values.count_of_categorical_values,
            CQCLClean.cqc_sector: CatValues.sector_column_values.count_of_categorical_values,
            CQCLClean.registration_status: CatValues.registration_status_column_values.count_of_categorical_values,
            CQCLClean.dormancy: CatValues.dormancy_column_values.count_of_categorical_values,
            CQCLClean.primary_service_type: CatValues.primary_service_type_column_values.count_of_categorical_values,
            CQCLClean.contemporary_cssr: CatValues.contemporary_cssr_column_values.count_of_categorical_values,
            CQCLClean.contemporary_region: CatValues.contemporary_region_column_values.count_of_categorical_values,
            CQCLClean.current_cssr: CatValues.current_cssr_column_values.count_of_categorical_values,
            CQCLClean.current_region: CatValues.current_region_column_values.count_of_categorical_values,
            CQCLClean.current_rural_urban_ind_11: CatValues.current_rui_column_values.count_of_categorical_values,
            CQCLClean.related_location: CatValues.related_location_column_values.count_of_categorical_values,
        },
        RuleName.custom_type: CustomValidationRules.care_home_and_primary_service_type,
    }
