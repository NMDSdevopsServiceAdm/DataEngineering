from dataclasses import dataclass

from utils.column_names.cleaned_data_files.ons_cleaned_values import (
    OnsCleanedColumns as ONSClean,
)

from utils.validation.validation_rule_names import RuleNames as RuleName
from utils.validation.categorical_column_values import (
    ONSCategoricalValues,
    ONSDistinctValues,
)


@dataclass
class PostcodeDirectoryCleanedValidationRules:
    rules_to_check = {
        RuleName.size_of_dataset: None,
        RuleName.complete_columns: [
            ONSClean.postcode,
            ONSClean.contemporary_ons_import_date,
            ONSClean.contemporary_cssr,
            ONSClean.contemporary_region,
            ONSClean.current_ons_import_date,
            ONSClean.current_cssr,
            ONSClean.current_region,
            ONSClean.current_rural_urban_ind_11,
        ],
        RuleName.index_columns: [
            ONSClean.postcode,
            ONSClean.contemporary_ons_import_date,
        ],
        RuleName.categorical_values_in_columns: {
            ONSClean.contemporary_cssr: ONSCategoricalValues.cssrs,
            ONSClean.contemporary_region: ONSCategoricalValues.regions,
            ONSClean.current_cssr: ONSCategoricalValues.cssrs,
            ONSClean.current_region: ONSCategoricalValues.regions,
            ONSClean.current_rural_urban_ind_11: ONSCategoricalValues.rural_urban_indicators,
        },
        RuleName.distinct_values: {
            ONSClean.contemporary_cssr: ONSDistinctValues.cssrs,
            ONSClean.contemporary_region: ONSDistinctValues.regions,
            ONSClean.current_cssr: ONSDistinctValues.cssrs,
            ONSClean.current_region: ONSDistinctValues.regions,
            ONSClean.current_rural_urban_ind_11: ONSDistinctValues.rural_urban_indicators,
        },
    }
