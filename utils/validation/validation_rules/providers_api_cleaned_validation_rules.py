from dataclasses import dataclass

from utils.column_names.cleaned_data_files.cqc_provider_cleaned import (
    CqcProviderCleanedColumns as CQCPClean,
)
from utils.column_names.validation_table_columns import Validation
from utils.validation.validation_rule_names import RuleNames as RuleName


@dataclass
class ProvidersAPICleanedValidationRules:
    rules_to_check = {
        RuleName.size_of_dataset: None,
        RuleName.complete_columns: [
            CQCPClean.cqc_provider_import_date,
            CQCPClean.provider_id,
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
    }
