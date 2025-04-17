from dataclasses import dataclass

from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns,
)
from utils.column_values.categorical_column_values import MainJobRoleLabels

from utils.validation.validation_rule_names import RuleNames as RuleName


@dataclass
class EstimatedIndCqcFilledPostsByJobRoleValidationRules:
    min_value = 0.0
    max_value = 3000

    rules_to_check = {
        RuleName.size_of_dataset: None,
        RuleName.complete_columns: [
            IndCqcColumns.location_id,
            IndCqcColumns.ascwds_workplace_import_date,
            IndCqcColumns.cqc_location_import_date,
            IndCqcColumns.care_home,
            IndCqcColumns.primary_service_type,
            IndCqcColumns.current_ons_import_date,
            IndCqcColumns.current_cssr,
            IndCqcColumns.current_region,
            IndCqcColumns.unix_time,
            IndCqcColumns.estimate_filled_posts,
            IndCqcColumns.estimate_filled_posts_source,
            IndCqcColumns.ascwds_job_role_ratios_merged,
            IndCqcColumns.ascwds_job_role_ratios_merged_source,
            IndCqcColumns.estimate_filled_posts_by_job_role,
            MainJobRoleLabels.activites_worker,
            MainJobRoleLabels.admin_staff,
            MainJobRoleLabels.advocacy,
            MainJobRoleLabels.allied_health_professional,
            MainJobRoleLabels.ancillary_staff,
            MainJobRoleLabels.assessment_officer,
            MainJobRoleLabels.care_coordinator,
            MainJobRoleLabels.care_worker,
            MainJobRoleLabels.childrens_roles,
            MainJobRoleLabels.community_support_and_outreach,
            MainJobRoleLabels.data_analyst,
            MainJobRoleLabels.data_governance_manager,
            MainJobRoleLabels.deputy_manager,
            MainJobRoleLabels.employment_support,
            MainJobRoleLabels.first_line_manager,
            MainJobRoleLabels.it_and_digital_support,
            MainJobRoleLabels.it_manager,
            MainJobRoleLabels.it_service_desk_manager,
            MainJobRoleLabels.learning_and_development_lead,
            MainJobRoleLabels.middle_management,
            MainJobRoleLabels.nursing_assistant,
            MainJobRoleLabels.occupational_therapist,
            MainJobRoleLabels.occupational_therapist_assistant,
            MainJobRoleLabels.other_care_role,
            MainJobRoleLabels.other_managerial_staff,
            MainJobRoleLabels.other_non_care_related_staff,
            MainJobRoleLabels.registered_manager,
            MainJobRoleLabels.registered_nurse,
            MainJobRoleLabels.registered_nursing_associate,
            MainJobRoleLabels.safeguarding_officer,
            MainJobRoleLabels.senior_care_worker,
            MainJobRoleLabels.senior_management,
            MainJobRoleLabels.social_worker,
            MainJobRoleLabels.software_developer,
            MainJobRoleLabels.supervisor,
            MainJobRoleLabels.support_worker,
            MainJobRoleLabels.team_leader,
            MainJobRoleLabels.technician,
        ],
        RuleName.index_columns: [
            IndCqcColumns.location_id,
            IndCqcColumns.cqc_location_import_date,
        ],
        RuleName.min_values: {
            MainJobRoleLabels.activites_worker: min_value,
            MainJobRoleLabels.admin_staff: min_value,
            MainJobRoleLabels.advocacy: min_value,
            MainJobRoleLabels.allied_health_professional: min_value,
            MainJobRoleLabels.ancillary_staff: min_value,
            MainJobRoleLabels.assessment_officer: min_value,
            MainJobRoleLabels.care_coordinator: min_value,
            MainJobRoleLabels.care_worker: min_value,
            MainJobRoleLabels.childrens_roles: min_value,
            MainJobRoleLabels.community_support_and_outreach: min_value,
            MainJobRoleLabels.data_analyst: min_value,
            MainJobRoleLabels.data_governance_manager: min_value,
            MainJobRoleLabels.deputy_manager: min_value,
            MainJobRoleLabels.employment_support: min_value,
            MainJobRoleLabels.first_line_manager: min_value,
            MainJobRoleLabels.it_and_digital_support: min_value,
            MainJobRoleLabels.it_manager: min_value,
            MainJobRoleLabels.it_service_desk_manager: min_value,
            MainJobRoleLabels.learning_and_development_lead: min_value,
            MainJobRoleLabels.middle_management: min_value,
            MainJobRoleLabels.nursing_assistant: min_value,
            MainJobRoleLabels.occupational_therapist: min_value,
            MainJobRoleLabels.occupational_therapist_assistant: min_value,
            MainJobRoleLabels.other_care_role: min_value,
            MainJobRoleLabels.other_managerial_staff: min_value,
            MainJobRoleLabels.other_non_care_related_staff: min_value,
            MainJobRoleLabels.registered_manager: min_value,
            MainJobRoleLabels.registered_nurse: min_value,
            MainJobRoleLabels.registered_nursing_associate: min_value,
            MainJobRoleLabels.safeguarding_officer: min_value,
            MainJobRoleLabels.senior_care_worker: min_value,
            MainJobRoleLabels.senior_management: min_value,
            MainJobRoleLabels.social_worker: min_value,
            MainJobRoleLabels.software_developer: min_value,
            MainJobRoleLabels.supervisor: min_value,
            MainJobRoleLabels.support_worker: min_value,
            MainJobRoleLabels.team_leader: min_value,
            MainJobRoleLabels.technician: min_value,
        },
        RuleName.max_values: {
            MainJobRoleLabels.activites_worker: max_value,
            MainJobRoleLabels.admin_staff: max_value,
            MainJobRoleLabels.advocacy: max_value,
            MainJobRoleLabels.allied_health_professional: max_value,
            MainJobRoleLabels.ancillary_staff: max_value,
            MainJobRoleLabels.assessment_officer: max_value,
            MainJobRoleLabels.care_coordinator: max_value,
            MainJobRoleLabels.care_worker: max_value,
            MainJobRoleLabels.childrens_roles: max_value,
            MainJobRoleLabels.community_support_and_outreach: max_value,
            MainJobRoleLabels.data_analyst: max_value,
            MainJobRoleLabels.data_governance_manager: max_value,
            MainJobRoleLabels.deputy_manager: max_value,
            MainJobRoleLabels.employment_support: max_value,
            MainJobRoleLabels.first_line_manager: max_value,
            MainJobRoleLabels.it_and_digital_support: max_value,
            MainJobRoleLabels.it_manager: max_value,
            MainJobRoleLabels.it_service_desk_manager: max_value,
            MainJobRoleLabels.learning_and_development_lead: max_value,
            MainJobRoleLabels.middle_management: max_value,
            MainJobRoleLabels.nursing_assistant: max_value,
            MainJobRoleLabels.occupational_therapist: max_value,
            MainJobRoleLabels.occupational_therapist_assistant: max_value,
            MainJobRoleLabels.other_care_role: max_value,
            MainJobRoleLabels.other_managerial_staff: max_value,
            MainJobRoleLabels.other_non_care_related_staff: max_value,
            MainJobRoleLabels.registered_manager: max_value,
            MainJobRoleLabels.registered_nurse: max_value,
            MainJobRoleLabels.registered_nursing_associate: max_value,
            MainJobRoleLabels.safeguarding_officer: max_value,
            MainJobRoleLabels.senior_care_worker: max_value,
            MainJobRoleLabels.senior_management: max_value,
            MainJobRoleLabels.social_worker: max_value,
            MainJobRoleLabels.software_developer: max_value,
            MainJobRoleLabels.supervisor: max_value,
            MainJobRoleLabels.support_worker: max_value,
            MainJobRoleLabels.team_leader: max_value,
            MainJobRoleLabels.technician: max_value,
        },
    }
