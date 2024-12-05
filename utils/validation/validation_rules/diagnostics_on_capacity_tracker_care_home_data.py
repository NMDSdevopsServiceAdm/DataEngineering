from dataclasses import dataclass

from utils.column_names.capacity_tracker_columns import (
    CapacityTrackerCareHomeCleanColumns as CTCHClean,
)
from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCQC,
)
from utils.column_values.categorical_columns_by_dataset import (
    DiagnosticsOnCapacityTrackerCareHomeCategoricalValues as CatValues,
)
from utils.validation.validation_rule_names import RuleNames as RuleName


@dataclass
class DiagnosticsOnCapacityTrackerCareHomeValidationRules:
    rules_to_check = {
        RuleName.complete_columns: [
            IndCQC.location_id,
            IndCQC.cqc_location_import_date,
            IndCQC.primary_service_type,
            CTCHClean.agency_and_non_agency_total_employed_imputed,
            IndCQC.estimate_source,
            IndCQC.estimate_value,
            IndCQC.distribution_mean,
            IndCQC.distribution_standard_deviation,
            IndCQC.distribution_kurtosis,
            IndCQC.distribution_skewness,
            IndCQC.residual,
            IndCQC.absolute_residual,
            IndCQC.percentage_residual,
            IndCQC.standardised_residual,
            IndCQC.average_absolute_residual,
            IndCQC.average_percentage_residual,
            IndCQC.max_residual,
            IndCQC.min_residual,
            IndCQC.percentage_of_residuals_within_absolute_value,
            IndCQC.percentage_of_residuals_within_percentage_value,
            IndCQC.percentage_of_standardised_residuals_within_limit,
        ],
        RuleName.index_columns: [
            IndCQC.location_id,
            IndCQC.cqc_location_import_date,
            IndCQC.primary_service_type,
            IndCQC.estimate_source,
        ],
        RuleName.min_values: {
            CTCHClean.agency_and_non_agency_total_employed_imputed: 0.0,
            IndCQC.estimate_value: -100.0,
            IndCQC.distribution_mean: 20.0,
            IndCQC.distribution_standard_deviation: 15.0,
            IndCQC.distribution_kurtosis: 1.0,
            IndCQC.distribution_skewness: 1.0,
            IndCQC.residual: -5000.0,
            IndCQC.absolute_residual: 0.0,
            IndCQC.percentage_residual: -99.0,
            IndCQC.standardised_residual: -100.0,
            IndCQC.average_absolute_residual: 0.0,
            IndCQC.average_percentage_residual: 0.0,
            IndCQC.max_residual: 0.0,
            IndCQC.min_residual: -5000.0,
            IndCQC.percentage_of_residuals_within_absolute_value: 0.25,
            IndCQC.percentage_of_residuals_within_percentage_value: 0.25,
            IndCQC.percentage_of_standardised_residuals_within_limit: 0.25,
        },
        RuleName.max_values: {
            CTCHClean.agency_and_non_agency_total_employed_imputed: 5000.0,
            IndCQC.estimate_value: 1000.0,
            IndCQC.distribution_mean: 100.0,
            IndCQC.distribution_standard_deviation: 40.0,
            IndCQC.distribution_kurtosis: 30.0,
            IndCQC.distribution_skewness: 3.0,
            IndCQC.residual: 5000.0,
            IndCQC.absolute_residual: 5000.0,
            IndCQC.percentage_residual: 99.0,
            IndCQC.standardised_residual: 200.0,
            IndCQC.average_absolute_residual: 1000.0,
            IndCQC.average_percentage_residual: 1.0,
            IndCQC.max_residual: 5000.0,
            IndCQC.min_residual: 0.0,
            IndCQC.percentage_of_residuals_within_absolute_value: 1.0,
            IndCQC.percentage_of_residuals_within_percentage_value: 1.0,
            IndCQC.percentage_of_standardised_residuals_within_limit: 1.0,
        },
        RuleName.categorical_values_in_columns: {
            IndCQC.primary_service_type: CatValues.primary_service_type_column_values.categorical_values,
            IndCQC.estimate_source: CatValues.estimate_source_column_values.categorical_values,
        },
        RuleName.distinct_values: {
            IndCQC.primary_service_type: CatValues.primary_service_type_column_values.count_of_categorical_values,
            IndCQC.estimate_source: CatValues.estimate_source_column_values.count_of_categorical_values,
        },
    }
