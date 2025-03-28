from dataclasses import dataclass

from utils.column_names.cleaned_data_files.ascwds_worker_cleaned import (
    AscwdsWorkerCleanedColumns as AWKClean,
)
from utils.column_values.categorical_columns_by_dataset import (
    ASCWDSWorkerCleanedCategoricalValues as CatValues,
)
from utils.validation.validation_rule_names import RuleNames as RuleName


@dataclass
class ASCWDSWorkerCleanedValidationRules:
    rules_to_check = {
        RuleName.complete_columns: [
            AWKClean.establishment_id,
            AWKClean.worker_id,
            AWKClean.main_job_role_clean,
            AWKClean.main_job_role_clean_labelled,
            AWKClean.ascwds_worker_import_date,
        ],
        RuleName.index_columns: [
            AWKClean.worker_id,
            AWKClean.ascwds_worker_import_date,
        ],
        RuleName.categorical_values_in_columns: {
            AWKClean.main_job_role_clean: CatValues.main_job_role_id_column_values.categorical_values,
            AWKClean.main_job_role_clean_labelled: CatValues.main_job_role_labels_column_values.categorical_values,
        },
        RuleName.distinct_values: {
            AWKClean.main_job_role_clean: CatValues.main_job_role_id_column_values.count_of_categorical_values,
            AWKClean.main_job_role_clean_labelled: CatValues.main_job_role_labels_column_values.count_of_categorical_values,
        },
    }
