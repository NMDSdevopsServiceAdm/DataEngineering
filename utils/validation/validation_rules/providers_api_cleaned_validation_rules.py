from dataclasses import dataclass

from utils.column_names.cleaned_data_files.cqc_provider_cleaned_values import (
    CqcProviderCleanedColumns as CQCPClean,
)
from utils.column_values.categorical_columns_by_dataset import (
    ProvidersApiCleanedCategoricalValues as CatValues,
)
from utils.validation.validation_rule_names import RuleNames as RuleName


@dataclass
class ProvidersAPICleanedValidationRules:
    rules_to_check = {
        RuleName.size_of_dataset: None,
        RuleName.complete_columns: [
            CQCPClean.cqc_provider_import_date,
            CQCPClean.provider_id,
            CQCPClean.cqc_sector,
            CQCPClean.name,
        ],
        RuleName.index_columns: [
            CQCPClean.provider_id,
            CQCPClean.cqc_provider_import_date,
        ],
        RuleName.categorical_values_in_columns: {
            CQCPClean.cqc_sector: CatValues.sector_column_values.categorical_values,
        },
        RuleName.distinct_values: {
            CQCPClean.cqc_sector: CatValues.sector_column_values.count_of_categorical_values,
        },
    }
