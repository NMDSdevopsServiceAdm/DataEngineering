from dataclasses import dataclass
import time

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
class EstimatedIndCqcFilledPostsValidationRules:
    rules_to_check = {
        RuleName.size_of_dataset: None,
        RuleName.complete_columns: [
            IndCqcColumns.location_id,
            IndCqcColumns.ascwds_workplace_import_date,
            IndCqcColumns.cqc_location_import_date,
            IndCqcColumns.care_home,
            IndCqcColumns.cqc_sector,
            IndCqcColumns.primary_service_type,
            IndCqcColumns.number_of_beds,
            IndCqcColumns.current_ons_import_date,
            IndCqcColumns.current_cssr,
            IndCqcColumns.current_region,
            IndCqcColumns.unix_time,
            IndCqcColumns.estimate_filled_posts,
            IndCqcColumns.estimate_filled_posts_source,
        ],
        RuleName.index_columns: [
            IndCqcColumns.location_id,
            IndCqcColumns.cqc_location_import_date,
        ],
        RuleName.min_values: {
            IndCqcColumns.number_of_beds: 1,
            IndCqcColumns.people_directly_employed: 1,
            IndCqcColumns.total_staff_bounded: 1,
            IndCqcColumns.worker_records_bounded: 1,
            IndCqcColumns.unix_time: 1262304000,  # 1st Jan 2010 in unix time
            IndCqcColumns.estimate_filled_posts: 1.0,
            IndCqcColumns.people_directly_employed_dedup: 1,
            IndCqcColumns.ascwds_filled_posts: 1.0,
            IndCqcColumns.ascwds_filled_posts_dedup_clean: 1.0,
            IndCqcColumns.rolling_average_model: 0.0,
            IndCqcColumns.extrapolation_model: 0.0,
            IndCqcColumns.interpolation_model: 0.0,
            IndCqcColumns.care_home_model: 0.0,
            IndCqcColumns.non_res_model: 0.0,
        },
        RuleName.max_values: {
            IndCqcColumns.number_of_beds: 500,
            IndCqcColumns.people_directly_employed: 10000,
            IndCqcColumns.total_staff_bounded: 3000,
            IndCqcColumns.worker_records_bounded: 3000,
            IndCqcColumns.unix_time: int(time.time()),  # current unix time
            IndCqcColumns.estimate_filled_posts: 3000.0,
            IndCqcColumns.people_directly_employed_dedup: 10000,
            IndCqcColumns.ascwds_filled_posts: 3000.0,
            IndCqcColumns.ascwds_filled_posts_dedup_clean: 3000.0,
            IndCqcColumns.rolling_average_model: 3000.0,
            IndCqcColumns.extrapolation_model: 3000.0,
            IndCqcColumns.interpolation_model: 3000.0,
            IndCqcColumns.care_home_model: 3000.0,
            IndCqcColumns.non_res_model: 3000.0,
        },
        RuleName.categorical_values_in_columns: {
            IndCqcColumns.care_home: CQCCategoricalValues.care_home_values,
            IndCqcColumns.primary_service_type: CQCCategoricalValues.primary_service_types,
            IndCqcColumns.current_cssr: ONSCategoricalValues.cssrs,
            IndCqcColumns.current_region: ONSCategoricalValues.regions,
            IndCqcColumns.ascwds_filled_posts_source: IndCQCCategoricalValues.ascwds_filled_posts_source,
            IndCqcColumns.estimate_filled_posts_source: IndCQCCategoricalValues.estimate_filled_posts_source,
        },
        RuleName.distinct_values: {
            IndCqcColumns.care_home: CQCDistinctValues.care_home_values,
            IndCqcColumns.primary_service_type: CQCDistinctValues.primary_service_types,
            IndCqcColumns.current_cssr: ONSDistinctValues.cssrs,
            IndCqcColumns.current_region: ONSDistinctValues.regions,
            IndCqcColumns.ascwds_filled_posts_source: IndCQCDistinctValues.ascwds_filled_posts_source,
            IndCqcColumns.estimate_filled_posts_source: IndCQCDistinctValues.estimate_filled_posts_source,
        },
    }
