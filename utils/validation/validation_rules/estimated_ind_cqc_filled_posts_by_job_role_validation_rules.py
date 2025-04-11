from dataclasses import dataclass

from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns,
)

from utils.validation.validation_rule_names import RuleNames as RuleName


@dataclass
class EstimatedIndCqcFilledPostsByJobRoleValidationRules:
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
            IndCqcColumns.activites_worker,
            IndCqcColumns.admin_staff,
            IndCqcColumns.advocacy,
            IndCqcColumns.allied_health_professional,
            IndCqcColumns.ancillary_staff,
            IndCqcColumns.assessment_officer,
            IndCqcColumns.care_coordinator,
            IndCqcColumns.care_worker,
            IndCqcColumns.childrens_roles,
            IndCqcColumns.community_support_and_outreach,
            IndCqcColumns.data_analyst,
            IndCqcColumns.data_governance_manager,
            IndCqcColumns.deputy_manager,
            IndCqcColumns.employment_support,
            IndCqcColumns.first_line_manager,
            IndCqcColumns.it_and_digital_support,
            IndCqcColumns.it_manager,
            IndCqcColumns.it_service_desk_manager,
            IndCqcColumns.learning_and_development_lead,
            IndCqcColumns.middle_management,
            IndCqcColumns.nursing_assistant,
            IndCqcColumns.occupational_therapist,
            IndCqcColumns.occupational_therapist_assistant,
            IndCqcColumns.other_care_role,
            IndCqcColumns.other_managerial_staff,
            IndCqcColumns.other_non_care_related_staff,
            IndCqcColumns.registered_manager,
            IndCqcColumns.registered_nurse,
            IndCqcColumns.registered_nursing_associate,
            IndCqcColumns.safeguarding_officer,
            IndCqcColumns.senior_care_worker,
            IndCqcColumns.senior_management,
            IndCqcColumns.social_worker,
            IndCqcColumns.software_developer,
            IndCqcColumns.supervisor,
            IndCqcColumns.support_worker,
            IndCqcColumns.team_leader,
            IndCqcColumns.technician,
        ],
        RuleName.index_columns: [
            IndCqcColumns.location_id,
            IndCqcColumns.cqc_location_import_date,
        ],
    }
