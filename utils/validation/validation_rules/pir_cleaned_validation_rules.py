from dataclasses import dataclass

from utils.column_names.cleaned_data_files.cqc_pir_cleaned_values import (
    CqcPIRCleanedColumns as CQCPIRClean,
)

from utils.validation.validation_rule_names import RuleNames as RuleName
from utils.validation.categorical_column_values import (
    CQCCategoricalValues,
    CQCDistinctValues,
)


@dataclass
class PIRCleanedValidationRules:
    rules_to_check = {
        RuleName.complete_columns: [
            CQCPIRClean.cqc_pir_import_date,
            CQCPIRClean.location_id,
            CQCPIRClean.people_directly_employed,
            CQCPIRClean.care_home,
        ],
        RuleName.index_columns: [
            CQCPIRClean.location_id,
            CQCPIRClean.cqc_pir_import_date,
        ],
        RuleName.max_values: {
            CQCPIRClean.people_directly_employed: 10000,
        },
        RuleName.min_values: {
            CQCPIRClean.people_directly_employed: 0,
        },
        RuleName.categorical_values_in_columns: {
            CQCPIRClean.care_home: CQCCategoricalValues.care_home_values,
        },
        RuleName.distinct_values: {
            CQCPIRClean.care_home: CQCDistinctValues.care_home_values,
        },
    }
