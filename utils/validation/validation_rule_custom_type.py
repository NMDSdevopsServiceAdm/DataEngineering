from dataclasses import dataclass

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_values.categorical_column_values import (
    CareHome,
    PrimaryServiceType,
    MainJobRoleLabels,
)
from utils.validation.validation_rule_names import CustomTypeArguments
from projects._03_independent_cqc._07_estimate_filled_posts_by_job_role.utils.job_role_validation_limits import (
    JobRoleLimitsPerPrimaryServiceType,
)


@dataclass
class CustomValidationRules:
    care_home_and_primary_service_type = {
        CustomTypeArguments.column_condition: f"({IndCQC.care_home} = '{CareHome.not_care_home}' AND {IndCQC.primary_service_type} = '{PrimaryServiceType.non_residential}') OR ({IndCQC.care_home} = '{CareHome.care_home}' AND {IndCQC.primary_service_type} = '{PrimaryServiceType.care_home_with_nursing}') OR ({IndCQC.care_home} = '{CareHome.care_home}' AND {IndCQC.primary_service_type} = '{PrimaryServiceType.care_home_only}')",
        CustomTypeArguments.constraint_name: "care_home_and_primary_service_type",
        CustomTypeArguments.hint: "The data in carehome and primary_service_type should be related.",
    }

    job_role_limit_care_home_with_nursing_advocacy = {
        CustomTypeArguments.column_condition: f"({IndCQC.primary_service_type} = '{PrimaryServiceType.care_home_with_nursing}' AND {MainJobRoleLabels.advocacy} < {JobRoleLimitsPerPrimaryServiceType.limit_care_home_with_nursing_advocacy}) AND ({IndCQC.primary_service_type} = '{PrimaryServiceType.care_home_only}' AND {MainJobRoleLabels.advocacy} < {JobRoleLimitsPerPrimaryServiceType.limit_care_only_home_advocacy}) AND ({IndCQC.primary_service_type} = '{PrimaryServiceType.non_residential}' AND {MainJobRoleLabels.advocacy} < {JobRoleLimitsPerPrimaryServiceType.limit_non_residential_advocacy})",
        CustomTypeArguments.constraint_name: "job_role_limit_per_primary_service_advocacy",
        CustomTypeArguments.hint: f"The estimated filled posts have exceeded the limit at care homes with nursing for advocacy role",
    }
