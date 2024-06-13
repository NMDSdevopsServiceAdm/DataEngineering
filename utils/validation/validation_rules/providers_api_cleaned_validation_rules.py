from dataclasses import dataclass

from utils.column_names.cleaned_data_files.cqc_provider_cleaned import (
    CqcProviderCleanedColumns as CQCPClean,
)
from utils.column_values.categorical_columns_by_dataset import (
    ProvidersApiCleanedCategoricalValues as CatValues,
)
from utils.column_names.validation_table_columns import (
    Validation,
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
        RuleName.min_values: {
            Validation.provider_id_length: 3,
        },
        RuleName.max_values: {
            Validation.provider_id_length: 14,
        },
        RuleName.categorical_values_in_columns: {
            CQCPClean.cqc_sector: CatValues.sector_column_values.categorical_values,
        },
        RuleName.distinct_values: {
            CQCPClean.cqc_sector: CatValues.sector_column_values.count_of_categorical_values,
        },
    }
