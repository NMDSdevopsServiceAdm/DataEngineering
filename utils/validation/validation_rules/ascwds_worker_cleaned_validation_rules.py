from dataclasses import dataclass

from utils.column_names.cleaned_data_files.ascwds_worker_cleaned_values import (
    AscwdsWorkerCleanedColumns as AWKClean,
)
from utils.column_values.categorical_columns_by_dataset import (
    ASCWDSWorkerCleanedCategoricalValues as CatValues,
)
from utils.validation.validation_rule_names import RuleNames as RuleName
from utils.value_labels.ascwds_worker.ascwds_worker_mainjrid import (
    AscwdsWorkerValueLabelsMainjrid as ValueCodes,
)


@dataclass
class ASCWDSWorkerCleanedValidationRules:
    rules_to_check = {
        RuleName.complete_columns: [
            AWKClean.establishment_id,
            AWKClean.worker_id,
            AWKClean.main_job_role_id,
            AWKClean.main_job_role_labelled,
            AWKClean.ascwds_worker_import_date,
        ],
        RuleName.index_columns: [
            AWKClean.worker_id,
            AWKClean.ascwds_worker_import_date,
        ],
        RuleName.categorical_values_in_columns: {
            AWKClean.main_job_role_id: CatValues.main_job_role_id_column_values.categorical_values,
            AWKClean.main_job_role_labelled: CatValues.main_job_role_labels_column_values.categorical_values,
        },
        RuleName.distinct_values: {
            AWKClean.main_job_role_id: CatValues.main_job_role_id_column_values.count_of_categorical_values,
            AWKClean.main_job_role_labelled: CatValues.main_job_role_labels_column_values.count_of_categorical_values,
        },
    }
