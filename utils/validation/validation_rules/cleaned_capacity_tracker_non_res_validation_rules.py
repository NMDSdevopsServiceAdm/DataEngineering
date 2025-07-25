from dataclasses import dataclass

from utils.column_names.capacity_tracker_columns import (
    CapacityTrackerNonResCleanColumns as CTNRClean,
)
from utils.validation.validation_rule_names import RuleNames as RuleName


@dataclass
class CleanedCapacityTrackerNonResValidationRules:
    rules_to_check = {
        RuleName.size_of_dataset: None,
        RuleName.complete_columns: [
            CTNRClean.cqc_id,
            CTNRClean.ct_non_res_import_date,
        ],
        RuleName.index_columns: [
            CTNRClean.cqc_id,
            CTNRClean.ct_non_res_import_date,
        ],
        RuleName.min_values: {
            CTNRClean.cqc_care_workers_employed: 1,
            CTNRClean.service_user_count: 1,
        },
        RuleName.max_values: {
            CTNRClean.cqc_care_workers_employed: 3000,
            CTNRClean.service_user_count: 3000,
        },
    }
