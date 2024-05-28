from dataclasses import dataclass

from utils.column_names.cleaned_data_files.cqc_provider_cleaned_values import (
    CqcProviderCleanedColumns as CQCPClean,
)

from utils.validation.validation_rule_names import RuleNames as RuleName
from utils.validation.categorical_column_values import (
    CQCCategoricalValues,
    CQCDistinctValues,
)


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
            CQCPClean.cqc_sector: CQCCategoricalValues.cqc_sector,
        },
        RuleName.distinct_values: {
            CQCPClean.cqc_sector: CQCDistinctValues.cqc_sector_values,
        },
    }
