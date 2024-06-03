from dataclasses import dataclass

from utils.column_names.cleaned_data_files.ascwds_workplace_cleaned import (
    AscwdsWorkplaceCleanedColumns as AWPClean,
)

from utils.validation.validation_rule_names import RuleNames as RuleName


@dataclass
class ASCWDSWorkplaceCleanedValidationRules:
    rules_to_check = {
        RuleName.complete_columns: [
            AWPClean.organisation_id,
            AWPClean.ascwds_workplace_import_date,
            AWPClean.establishment_id,
        ],
        RuleName.index_columns: [
            AWPClean.establishment_id,
            AWPClean.ascwds_workplace_import_date,
        ],
        RuleName.max_values: {
            AWPClean.total_staff_bounded: 3000,
            AWPClean.worker_records_bounded: 3000,
        },
        RuleName.min_values: {
            AWPClean.total_staff_bounded: 1,
            AWPClean.worker_records_bounded: 1,
        },
    }
