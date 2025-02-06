from pyspark.sql import DataFrame, functions as F

from utils.column_names.cleaned_data_files.ascwds_worker_cleaned import (
    AscwdsWorkerCleanedColumns as AWKClean,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_values.categorical_column_values import MainJobRoleLabels

list_of_job_roles = [
    MainJobRoleLabels.not_known,
    MainJobRoleLabels.senior_management,
    MainJobRoleLabels.middle_management,
    MainJobRoleLabels.first_line_manager,
    MainJobRoleLabels.registered_manager,
    MainJobRoleLabels.supervisor,
    MainJobRoleLabels.social_worker,
    MainJobRoleLabels.senior_care_worker,
    MainJobRoleLabels.care_worker,
    MainJobRoleLabels.community_support_and_outreach,
    MainJobRoleLabels.employment_support,
    MainJobRoleLabels.advocacy,
    MainJobRoleLabels.occupational_therapist,
    MainJobRoleLabels.registered_nurse,
    MainJobRoleLabels.allied_health_professional,
    MainJobRoleLabels.technician,
    MainJobRoleLabels.other_care_role,
    MainJobRoleLabels.care_related_staff,
    MainJobRoleLabels.admin_staff,
    MainJobRoleLabels.ancillary_staff,
    MainJobRoleLabels.other_non_care_related_staff,
    MainJobRoleLabels.activites_worker,
    MainJobRoleLabels.safeguarding_officer,
    MainJobRoleLabels.occupational_therapist_assistant,
    MainJobRoleLabels.registered_nursing_associate,
    MainJobRoleLabels.nursing_assistant,
    MainJobRoleLabels.assessment_officer,
    MainJobRoleLabels.care_coordinator,
    MainJobRoleLabels.childrens_roles,
    MainJobRoleLabels.deputy_manager,
    MainJobRoleLabels.learning_and_development_lead,
    MainJobRoleLabels.team_leader,
    MainJobRoleLabels.data_analyst,
    MainJobRoleLabels.data_governance_manager,
    MainJobRoleLabels.it_and_digital_support,
    MainJobRoleLabels.it_manager,
    MainJobRoleLabels.it_service_desk_manager,
    MainJobRoleLabels.software_developer,
    MainJobRoleLabels.support_worker,
]


def count_job_role_per_establishment_as_columns(df: DataFrame) -> DataFrame:
    """
    Group the worker dataset by establishment id and import date.
    Subsequently performs a pivot on the clean job role labels which will be the additional columns in the grouped data.
    Any labels in the list of job roles not present in the establishment will have their counts aggregated to zero.

    Args:
        df (DataFrame): A dataframe containing cleaned ASC-WDS worker data.

    Returns:
        DataFrame: A dataframe with unique establishmentid and import date.
    """

    list_of_job_roles

    df = (
        df.groupBy(
            F.col(AWKClean.establishment_id),
            F.col(AWKClean.ascwds_worker_import_date),
        )
        .pivot(AWKClean.main_job_role_clean_labelled, list_of_job_roles)
        .count()
    )

    df = df.na.fill(0)
    return df
