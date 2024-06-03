from dataclasses import dataclass

from utils.column_names.cleaned_data_files.ons_cleaned import (
    OnsCleanedColumns as ONSClean,
)
from utils.column_values.categorical_columns_by_dataset import (
    PostcodeDirectoryCleanedCategoricalValues as CatValues,
)
from utils.validation.validation_rule_names import RuleNames as RuleName


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
            ONSClean.contemporary_cssr: CatValues.contemporary_cssr_column_values.categorical_values,
            ONSClean.contemporary_region: CatValues.contemporary_region_column_values.categorical_values,
            ONSClean.current_cssr: CatValues.current_cssr_column_values.categorical_values,
            ONSClean.current_region: CatValues.current_region_column_values.categorical_values,
            ONSClean.current_rural_urban_ind_11: CatValues.current_rui_column_values.categorical_values,
        },
        RuleName.distinct_values: {
            ONSClean.contemporary_cssr: CatValues.contemporary_cssr_column_values.count_of_categorical_values,
            ONSClean.contemporary_region: CatValues.contemporary_region_column_values.count_of_categorical_values,
            ONSClean.current_cssr: CatValues.current_cssr_column_values.count_of_categorical_values,
            ONSClean.current_region: CatValues.current_region_column_values.count_of_categorical_values,
            ONSClean.current_rural_urban_ind_11: CatValues.current_rui_column_values.count_of_categorical_values,
        },
    }
