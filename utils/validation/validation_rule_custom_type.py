from dataclasses import dataclass

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_values.categorical_column_values import (
    CareHome,
    PrimaryServiceType,
    PrimaryServiceTypeSecondLevel,
)
from utils.validation.validation_rule_names import CustomTypeArguments


@dataclass
class CustomValidationRules:
    care_home_and_primary_service_type = {
        CustomTypeArguments.column_condition: f"({IndCQC.care_home} = '{CareHome.not_care_home}' AND {IndCQC.primary_service_type} = '{PrimaryServiceType.non_residential}') OR ({IndCQC.care_home} = '{CareHome.care_home}' AND {IndCQC.primary_service_type} = '{PrimaryServiceType.care_home_with_nursing}') OR ({IndCQC.care_home} = '{CareHome.care_home}' AND {IndCQC.primary_service_type} = '{PrimaryServiceType.care_home_only}')",
        CustomTypeArguments.constraint_name: "care_home_and_primary_service_type",
        CustomTypeArguments.hint: "The data in carehome and primary_service_type should be related.",
    }

    primary_service_type_and_primary_service_type_second_level = {
        CustomTypeArguments.column_condition: f"({IndCQC.primary_service_type} = '{PrimaryServiceType.care_home_with_nursing}' AND {IndCQC.primary_service_type_second_level} = '{PrimaryServiceTypeSecondLevel.shared_lives}') OR ({IndCQC.primary_service_type} = '{PrimaryServiceType.care_home_only}' AND {IndCQC.primary_service_type_second_level} = '{PrimaryServiceTypeSecondLevel.shared_lives}') OR ({IndCQC.primary_service_type} = '{PrimaryServiceType.non_residential}' AND {IndCQC.primary_service_type_second_level} = '{PrimaryServiceTypeSecondLevel.shared_lives}') OR ({IndCQC.primary_service_type} = '{PrimaryServiceType.non_residential}' AND {IndCQC.primary_service_type_second_level} = '{PrimaryServiceTypeSecondLevel.non_residential}') OR ({IndCQC.primary_service_type} = '{PrimaryServiceType.non_residential}' AND {IndCQC.primary_service_type_second_level} = '{PrimaryServiceTypeSecondLevel.other_residential}') OR ({IndCQC.primary_service_type} = '{PrimaryServiceType.non_residential}' AND {IndCQC.primary_service_type_second_level} = '{PrimaryServiceTypeSecondLevel.other_non_residential}') OR ({IndCQC.primary_service_type} = '{PrimaryServiceType.care_home_with_nursing}' AND {IndCQC.primary_service_type_second_level} = '{PrimaryServiceTypeSecondLevel.care_home_with_nursing}') OR ({IndCQC.primary_service_type} = '{PrimaryServiceType.care_home_only}' AND {IndCQC.primary_service_type_second_level} = '{PrimaryServiceTypeSecondLevel.care_home_only}')",
        CustomTypeArguments.constraint_name: "primary_service_type_and_primary_service_type_second_level",
        CustomTypeArguments.hint: "The data in primary_service_type and primary_service_type_second_level should be related.",
    }
