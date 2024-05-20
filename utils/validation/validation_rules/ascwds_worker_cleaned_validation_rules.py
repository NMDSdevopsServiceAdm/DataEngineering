from dataclasses import dataclass

from utils.column_names.cleaned_data_files.ascwds_worker_cleaned_values import (
    AscwdsWorkerCleanedColumns as AWKClean,
)

from utils.validation.validation_rule_names import RuleNames as RuleName
from utils.validation.categorical_column_values import (
    ASCWDSCategoricalValues,
    ASCWDSDistinctValues,
)


@dataclass
class PIRCleanedValidationRules:
    rules_to_check = {
        RuleName.complete_columns: [
            AWKClean.establishment_id,
            AWKClean.location_id,
            AWKClean.main_job_role_id,
            AWKClean.main_job_role_labelled,
            AWKClean.ascwds_worker_import_date,
        ],
        RuleName.index_columns: [
            AWKClean.location_id,
            AWKClean.ascwds_worker_import_date,
        ],
        RuleName.categorical_values_in_columns: {
            AWKClean.main_job_role_id: ASCWDSCategoricalValues.main_job_role_id,
            AWKClean.main_job_role_labelled: ASCWDSCategoricalValues.main_job_role_labelled,
        },
        RuleName.distinct_values: {
            AWKClean.main_job_role_id: ASCWDSDistinctValues.main_job_role_id_values,
            AWKClean.main_job_role_labelled: ASCWDSDistinctValues.main_job_role_labelled_values,
        },
    }
