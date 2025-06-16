from dataclasses import dataclass

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_values.categorical_column_values import (
    CareHome,
    PrimaryServiceType,
    PrimaryServiceTypeSecondLevel,
    Services,
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
        CustomTypeArguments.column_condition: f"('{Services.shared_lives}' IN {IndCQC.services_offered} AND {IndCQC.primary_service_type_second_level} = '{PrimaryServiceTypeSecondLevel.shared_lives}') OR ('{Services.care_home_service_with_nursing}' IN {IndCQC.services_offered} AND {IndCQC.primary_service_type_second_level} = '{PrimaryServiceTypeSecondLevel.care_home_with_nursing}') OR ('{Services.care_home_service_without_nursing}' IN {IndCQC.services_offered} AND {IndCQC.primary_service_type_second_level} = '{PrimaryServiceTypeSecondLevel.care_home_only}') OR ('{Services.domiciliary_care_service}' IN {IndCQC.services_offered} AND {IndCQC.primary_service_type_second_level} = '{PrimaryServiceTypeSecondLevel.non_residential}') OR ('{Services.community_health_care_services_nurses_agency_only}' IN {IndCQC.services_offered} AND {IndCQC.primary_service_type_second_level} = '{PrimaryServiceTypeSecondLevel.non_residential}') OR ('{Services.supported_living_service}' IN {IndCQC.services_offered} AND {IndCQC.primary_service_type_second_level} = '{PrimaryServiceTypeSecondLevel.non_residential}') OR ('{Services.extra_care_housing_services}' IN {IndCQC.services_offered} AND {IndCQC.primary_service_type_second_level} = '{PrimaryServiceTypeSecondLevel.non_residential}') OR ('{Services.residential_substance_misuse_treatment_and_rehabilitation_service}' IN {IndCQC.services_offered} AND {IndCQC.primary_service_type_second_level} = '{PrimaryServiceTypeSecondLevel.other_residential}') OR ('{Services.hospice_services}' IN {IndCQC.services_offered} AND {IndCQC.primary_service_type_second_level} = '{PrimaryServiceTypeSecondLevel.other_residential}') OR ('{Services.acute_services_with_overnight_beds}' IN {IndCQC.services_offered} AND {IndCQC.primary_service_type_second_level} = '{PrimaryServiceTypeSecondLevel.other_residential}')",
        CustomTypeArguments.constraint_name: "primary_service_type_and_primary_service_type_second_level",
        CustomTypeArguments.hint: "The data in primary_service_type and primary_service_type_second_level should be related.",
    }
