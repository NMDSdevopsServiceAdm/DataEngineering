from datetime import date

import polars as pl

from polars_utils.column_types import CategoricalColumnTypes
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_values.categorical_column_values import MainJobRoleLabels
from utils.value_labels.ascwds_worker.ascwds_worker_jobgroup_dictionary import (
    AscwdsWorkerValueLabelsJobGroup,
)


def percentage_share_handling_zero_sum(column: str | pl.Expr) -> pl.Expr:
    """Calculate the percentage share of a column handling zero sum case.

    If the sum of all non-null values is zero, dividing by zero leads to a NaN.
    In this case we want to assume an even distribution across all non-null
    rows.

    Can be used in conjunction with `.group_by` and `.over` methods to get
    proportions within groups.
    """
    col = pl.col(column) if isinstance(column, str) else column
    total = col.sum()
    return (
        pl.when((total == 0) & (col == 0))
        .then(1 / col.is_not_null().sum())
        .otherwise(col / total)
    )


def add_job_role_groups_column(
    lf: pl.LazyFrame, job_group_column_name: str
) -> pl.LazyFrame:
    """
    Adds a new column with job role groups.

    Using an Enumerated column of job roles, this function creates a new column which assigns
    the job role group for each row to a new column.

    Args:
        lf(pl.LazyFrame): A lazy frame with the column main_job_role_clean_labelled.
        job_group_column_name(str): The name for the new job group column.

    Returns:
        pl.LazyFrame: A lazy frame with the job group column added.
    """
    job_role_group_data = {
        IndCQC.main_job_role_clean_labelled: list(
            AscwdsWorkerValueLabelsJobGroup.job_role_to_job_group_dict.keys()
        ),
        job_group_column_name: list(
            AscwdsWorkerValueLabelsJobGroup.job_role_to_job_group_dict.values()
        ),
    }
    job_role_group_schema = {
        IndCQC.main_job_role_clean_labelled: CategoricalColumnTypes.JobRoleEnumType,
        job_group_column_name: CategoricalColumnTypes.JobGroupEnumType,
    }
    job_role_group_lf = pl.LazyFrame(job_role_group_data, schema=job_role_group_schema)
    lf = lf.join(job_role_group_lf, on=IndCQC.main_job_role_clean_labelled, how="left")
    return lf


class HistoricJobRoleAdjustmentConfig:
    """
    A dictionary in which keys are dates before which we reallocate job roles.
        Date: {
            historic role to reallocate: {
                receiving role: amount to receive,
            },
        }

    Please note the registered manager role must not be a receiving role due to
    the order of functions in _04_estimate job roles script (historical reallocation
    happens after RM reassignment). This role is taken from CQC counts and therefore
    must be that count thereafter.
    """

    adjustment_dict = {
        date(2023, 8, 1): {
            MainJobRoleLabels.deputy_manager: {
                MainJobRoleLabels.care_worker: 0.2249,
                MainJobRoleLabels.first_line_manager: 0.4689,
                MainJobRoleLabels.senior_care_worker: 0.3062,
            },
            MainJobRoleLabels.learning_and_development_lead: {
                MainJobRoleLabels.other_non_care_related_staff: 1.0,
            },
            MainJobRoleLabels.team_leader: {
                MainJobRoleLabels.care_worker: 0.3446,
                MainJobRoleLabels.first_line_manager: 0.1350,
                MainJobRoleLabels.senior_care_worker: 0.1749,
                MainJobRoleLabels.supervisor: 0.3455,
            },
        },
        date(2024, 6, 1): {
            MainJobRoleLabels.data_analyst: {
                MainJobRoleLabels.other_non_care_related_staff: 1.0,
            },
            MainJobRoleLabels.data_governance_manager: {
                MainJobRoleLabels.other_managerial_staff: 1.0,
            },
            MainJobRoleLabels.it_and_digital_support: {
                MainJobRoleLabels.other_non_care_related_staff: 1.0,
            },
            MainJobRoleLabels.it_manager: {
                MainJobRoleLabels.other_managerial_staff: 1.0,
            },
            MainJobRoleLabels.it_service_desk_manager: {
                MainJobRoleLabels.other_managerial_staff: 1.0,
            },
            MainJobRoleLabels.software_developer: {
                MainJobRoleLabels.other_non_care_related_staff: 1.0,
            },
            MainJobRoleLabels.support_worker: {
                MainJobRoleLabels.care_worker: 0.7219,
                MainJobRoleLabels.community_support_and_outreach: 0.2537,
                MainJobRoleLabels.senior_care_worker: 0.0166,
                MainJobRoleLabels.activites_worker: 0.0078,
            },
        },
    }
