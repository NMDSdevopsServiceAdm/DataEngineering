from dataclasses import dataclass

from utils.column_names.capacity_tracker_columns import (
    CapacityTrackerCareHomeCleanColumns as CTCHClean,
)
from utils.validation.validation_rule_names import RuleNames as RuleName


@dataclass
class CleanedCapacityTrackerCareHomeValidationRules:
    rules_to_check = {
        RuleName.size_of_dataset: None,
        RuleName.complete_columns: [
            CTCHClean.cqc_id,
            CTCHClean.capacity_tracker_import_date,
        ],
        RuleName.index_columns: [
            CTCHClean.cqc_id,
            CTCHClean.capacity_tracker_import_date,
        ],
        RuleName.min_values: {
            CTCHClean.nurses_employed: 0,
            CTCHClean.care_workers_employed: 0,
            CTCHClean.non_care_workers_employed: 0,
            CTCHClean.agency_nurses_employed: 0,
            CTCHClean.agency_care_workers_employed: 0,
            CTCHClean.agency_non_care_workers_employed: 0,
            CTCHClean.non_agency_total_employed: 0,
            CTCHClean.agency_total_employed: 0,
            CTCHClean.agency_and_non_agency_total_employed: 1,
        },
        RuleName.max_values: {
            CTCHClean.nurses_employed: 1000,
            CTCHClean.care_workers_employed: 1000,
            CTCHClean.non_care_workers_employed: 1000,
            CTCHClean.agency_nurses_employed: 1000,
            CTCHClean.agency_care_workers_employed: 2500,
            CTCHClean.agency_non_care_workers_employed: 1000,
            CTCHClean.non_agency_total_employed: 1000,
            CTCHClean.agency_total_employed: 4000,
            CTCHClean.agency_and_non_agency_total_employed: 4000,
        },
    }
