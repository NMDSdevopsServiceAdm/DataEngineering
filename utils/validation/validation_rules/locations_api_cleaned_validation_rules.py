from dataclasses import dataclass

from utils.column_names.cleaned_data_files.cqc_location_cleaned_values import (
    CqcLocationCleanedColumns as CQCLClean,
)

from utils.validation.validation_rule_names import RuleNames as RuleName
from utils.validation.categorical_column_values import (
    CQCCategoricalValues,
    CQCDistinctValues,
    ONSCategoricalValues,
    ONSDistinctValues,
)


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
            CQCLClean.registration_date,
            CQCLClean.primary_service_type,
            CQCLClean.name,
            CQCLClean.services_offered,
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
            CQCLClean.number_of_beds: 1,
        },
        RuleName.max_values: {
            CQCLClean.number_of_beds: 500,
        },
        RuleName.categorical_values_in_columns: {
            CQCLClean.care_home: CQCCategoricalValues.care_home_values,
            CQCLClean.cqc_sector: CQCCategoricalValues.cqc_sector,
            CQCLClean.registration_status: CQCCategoricalValues.registration_status,
            CQCLClean.dormancy: CQCCategoricalValues.dormancy_values,
            CQCLClean.primary_service_type: CQCCategoricalValues.primary_service_types,
            CQCLClean.contemporary_cssr: ONSCategoricalValues.cssrs,
            CQCLClean.contemporary_region: ONSCategoricalValues.regions,
            CQCLClean.current_cssr: ONSCategoricalValues.cssrs,
            CQCLClean.current_region: ONSCategoricalValues.regions,
            CQCLClean.current_rural_urban_ind_11: ONSCategoricalValues.rural_urban_indicators,
        },
        RuleName.distinct_values: {
            CQCLClean.care_home: CQCDistinctValues.care_home_values,
            CQCLClean.cqc_sector: CQCDistinctValues.cqc_sector_values,
            CQCLClean.registration_status: CQCDistinctValues.registration_status_values,
            CQCLClean.dormancy: CQCDistinctValues.dormancy_values,
            CQCLClean.primary_service_type: CQCDistinctValues.primary_service_types,
            CQCLClean.contemporary_cssr: ONSDistinctValues.cssrs,
            CQCLClean.contemporary_region: ONSDistinctValues.regions,
            CQCLClean.current_cssr: ONSDistinctValues.cssrs,
            CQCLClean.current_region: ONSDistinctValues.regions,
            CQCLClean.current_rural_urban_ind_11: ONSDistinctValues.rural_urban_indicators,
        },
    }
