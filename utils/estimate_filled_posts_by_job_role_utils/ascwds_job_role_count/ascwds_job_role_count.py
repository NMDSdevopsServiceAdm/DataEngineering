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
    MainJobRoleLabels.support_worker
    ]


def count_job_role_per_establishment(df: DataFrame) -> DataFrame:
    """
    Counts the number of rows per establishmentid, import date and main job role.

    This function groups the ASC-WDS worker dataset by establishmentid, import date and main job role
    and adds a column with the count of rows per group.
    Duplicate rows by establishmentid, import date and main job role are removed.

    Args:
        df (DataFrame): A dataframe containing cleaned ASC-WDS worker data.

    Returns:
        DataFrame: A dataframe with unique establishmentid, import date and main job role and row count.
    """

    df = df.groupBy(
        F.col(AWKClean.establishment_id),
        F.col(AWKClean.ascwds_worker_import_date),
        F.col(AWKClean.main_job_role_clean_labelled),
    ).agg(
        F.count(F.col(AWKClean.main_job_role_clean_labelled)).alias(
            IndCQC.ascwds_main_job_role_counts
        )
    )
    return df


def convert_job_role_count_to_job_role_map(df: DataFrame) -> DataFrame:
    """
    Adds a column with a dictionary created from main job role and main job role count.

    Adds column which contains a dictionary. The keys are main job role and values are main job role count.
    Each dictionary is per establishmentid and import date.
    Main job role and main job role count columns are removed and duplicate rows by establishmentid and
    import date are removed.

    Args:
        df (DataFrame): A dataframe containing cleaned ASC-WDS worker data with a count per main job role.

    Returns:
        DataFrame: A dataframe with unique establishmentid, import date and dictionary where key = main job role value = main job role count.
    """
    struct_column: str = "struct_column"
    df_struct = df.withColumn(
        struct_column,
        F.struct(
            F.col(AWKClean.main_job_role_clean_labelled),
            F.col(IndCQC.ascwds_main_job_role_counts),
        ),
    )
    df_mapped = df_struct.groupBy(
        F.col(AWKClean.establishment_id), F.col(AWKClean.ascwds_worker_import_date)
    ).agg(
        F.map_from_entries(F.collect_list(struct_column)).alias(
            IndCQC.ascwds_main_job_role_counts
        )
    )
    return df_mapped

def count_job_role_per_establishment_as_columns(df: DataFrame) -> DataFrame:
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
