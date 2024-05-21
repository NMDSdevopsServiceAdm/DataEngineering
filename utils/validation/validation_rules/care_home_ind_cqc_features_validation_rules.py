from dataclasses import dataclass

from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns,
)

from utils.validation.validation_rule_names import RuleNames as RuleName
from utils.validation.categorical_column_values import (
    CQCCategoricalValues,
    CQCDistinctValues,
    ONSCategoricalValues,
    ONSDistinctValues,
    IndCQCCategoricalValues,
    IndCQCDistinctValues,
)


@dataclass
class CareHomeIndCqcFeaturesValidationRules:
    rules_to_check = {
        RuleName.size_of_dataset: None,
        RuleName.complete_columns: [
            IndCqcColumns.location_id,
            IndCqcColumns.cqc_location_import_date,
            IndCqcColumns.care_home,
            IndCqcColumns.number_of_beds,
            IndCqcColumns.features,
            IndCqcColumns.current_region,
        ],
        RuleName.index_columns: [
            IndCqcColumns.location_id,
            IndCqcColumns.cqc_location_import_date,
        ],
        RuleName.min_values: {
            IndCqcColumns.number_of_beds: 1,
            IndCqcColumns.people_directly_employed: 0,
            IndCqcColumns.ascwds_filled_posts_dedup_clean: 1.0,
        },
        RuleName.max_values: {
            IndCqcColumns.number_of_beds: 500,
            IndCqcColumns.people_directly_employed: 10000,
            IndCqcColumns.ascwds_filled_posts_dedup_clean: 3000.0,
        },
        RuleName.categorical_values_in_columns: {
            IndCqcColumns.care_home: CQCCategoricalValues.care_home_values,
            IndCqcColumns.current_region: ONSCategoricalValues.regions,
        },
        RuleName.distinct_values: {
            IndCqcColumns.care_home: CQCDistinctValues.care_home_values,
            IndCqcColumns.current_region: ONSDistinctValues.regions,
        },
    }
