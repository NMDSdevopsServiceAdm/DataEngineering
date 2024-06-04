from dataclasses import dataclass

from utils.column_names.raw_data_files.ascwds_worker_columns import (
    AscwdsWorkerColumns as AWK,
    PartitionKeys as Keys,
)
from utils.column_values.categorical_columns_by_dataset import (
    ASCWDSWorkerCleanedCategoricalValues as CatValues,
)
from utils.validation.validation_rule_names import RuleNames as RuleName


@dataclass
class ASCWDSWorkerRawValidationRules:
    rules_to_check = {
        RuleName.complete_columns: [
            AWK.establishment_id,
            AWK.worker_id,
            AWK.main_job_role_id,
            Keys.import_date,
        ],
        RuleName.index_columns: [
            AWK.worker_id,
            Keys.import_date,
        ],
        RuleName.categorical_values_in_columns: {
            AWK.main_job_role_id: CatValues.main_job_role_id_column_values.categorical_values,
        },
        RuleName.distinct_values: {
            AWK.main_job_role_id: CatValues.main_job_role_id_column_values.count_of_categorical_values,
        },
    }
