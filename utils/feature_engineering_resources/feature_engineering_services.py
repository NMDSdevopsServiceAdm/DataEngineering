from dataclasses import dataclass

from utils.column_values.categorical_columns_by_dataset import (
    FeatureEngineeringCategoricalValues as CatValues,
)


@dataclass
class FeatureEngineeringValueLabelsServices:
    """The possible values of the services feature in the independent CQC estimates pipeline"""

    column_name: str = CatValues.services_column_values.column_name

    labels_dict = {
        "service_acute_with_beds": CatValues.services_column_values.acute_services_with_overnight_beds,
        "service_acute_without_beds": CatValues.services_column_values.acute_services_without_overnight_beds,
        "service_ambulance": CatValues.services_column_values.ambulance_service,
        "service_care_home_with_nursing": CatValues.services_column_values.care_home_service_with_nursing,
        "service_care_home_without_nursing": CatValues.services_column_values.care_home_service_without_nursing,
        "service_community_substance_misuse": CatValues.services_column_values.community_based_services_for_people_who_misuse_substances,
        "service_community_learning_disability": CatValues.services_column_values.community_based_services_for_people_with_a_learning_disability,
        "service_community_mental_health": CatValues.services_column_values.community_based_services_for_people_with_mental_health_needs,
        "service_community_health_NA": CatValues.services_column_values.community_health_care_services_nurses_agency_only,
        "service_community_health": CatValues.services_column_values.community_healthcare_service,
        "service_dental": CatValues.services_column_values.dental_service,
        "service_diagnostic": CatValues.services_column_values.diagnostic_and_screening_service,
        "service_doctors_consultant": CatValues.services_column_values.doctors_consultation_service,
        "service_doctors_treatment": CatValues.services_column_values.doctors_treatment_service,
        "service_domiciliary": CatValues.services_column_values.domiciliary_care_service,
        "service_extra_care_housing": CatValues.services_column_values.extra_care_housing_services,
        "service_hospice_at_home": CatValues.services_column_values.hospice_services_at_home,
        "service_hospice": CatValues.services_column_values.hospice_services,
        "service_hospital": CatValues.services_column_values.hospital_services_for_people_with_mental_health_needs,
        "service_long_term": CatValues.services_column_values.long_term_conditions_services,
        "service_mobile_doctors": CatValues.services_column_values.mobile_doctors_service,
        "service_prison_healthcare": CatValues.services_column_values.prison_healthcare_services,
        "service_rehab": CatValues.services_column_values.rehabilitation_services,
        "service_remote_advice": CatValues.services_column_values.remote_clinical_advice_service,
        "service_residential_rehab": CatValues.services_column_values.residential_substance_misuse_treatment_and_rehabilitation_service,
        "service_shared_lives": CatValues.services_column_values.shared_lives,
        "service_specialist_college": CatValues.services_column_values.specialist_college_service,
        "service_supported_living": CatValues.services_column_values.supported_living_service,
        "service_urgent_care": CatValues.services_column_values.urgent_care_services,
    }
