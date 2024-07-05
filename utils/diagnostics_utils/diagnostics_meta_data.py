from dataclasses import dataclass

from utils.column_names.ind_cqc_pipeline_columns import (
    PartitionKeys as Keys,
    IndCqcColumns as IndCQC,
)


@dataclass
class Variables:
    care_home_with_nursing: str = "Care home with nursing"
    care_home_without_nursing: str = "Care home without nursing"
    non_residential: str = "non-residential"

    asc_wds: str = "ascwds"
    capacity_tracker: str = "capacity_tracker"
    pir: str = "pir"

    care_home: str = "care_home"
    non_res: str = "non_res"

    capacity_tracker_snapshot_date: str = "20230401"
    capacity_tracker_snapshot_date_formatted: str = "2023-04-01"


@dataclass
class Prefixes:
    avg: str = "avg_"
    residuals: str = "residuals_"


@dataclass
class CareWorkerToJobsRatio:
    care_worker_to_all_jobs_ratio: float = 1.3


@dataclass
class Columns:
    CQC_ID: str = "CQC_ID"
    NURSES_EMPLOYED: str = "Nurses_Employed"
    CARE_WORKERS_EMPLOYED: str = "Care_Workers_Employed"
    NON_CARE_WORKERS_EMPLOYED: str = "Non_Care_Workers_Employed"
    AGENCY_NURSES_EMPLOYED: str = "Agency_Nurses_Employed"
    AGENCY_CARE_WORKERS_EMPLOYED: str = "Agency_Care_Workers_Employed"
    AGENCY_NON_CARE_WORKERS_EMPLOYED: str = "Agency_Non_Care_Workers_Employed"
    CQC_CARE_WORKERS_EMPLOYED: str = "CQC_Care_Workers_Employed"
    CARE_HOME_EMPLOYED: str = "care_home_employed"
    NON_RESIDENTIAL_EMPLOYED: str = "non_residential_employed"
    DESCRIPTION_OF_CHANGES: str = "description_of_changes"
    VALUE: str = "value"
    ID: str = "id"
    RUN_TIMESTAMP: str = "run_timestamp"
    CAPACITY_TRACKER_CARE_HOMES_SNAPSHOT_DATE: str = (
        "capacity_tracker_care_homes_snapshot_date"
    )
    CAPACITY_TRACKER_NON_RESIDENTIAL_SNAPSHOT_DATE: str = (
        "capacity_tracker_non_residential_snapshot_date"
    )


@dataclass
class TestColumns:
    residuals_test_column_names = [
        "residuals_estimate_filled_posts_non_res_pir",
        "residuals_ascwds_filled_posts_clean_dedup_non_res_pir",
    ]


@dataclass
class ResidualsRequired:
    models = [
        IndCQC.estimate_filled_posts,
        IndCQC.ascwds_filled_posts_dedup_clean,
        IndCQC.rolling_average_model,
        IndCQC.care_home_model,
        IndCQC.extrapolation_care_home_model,
        IndCQC.interpolation_model,
        IndCQC.non_res_model,
    ]

    services = [
        Variables.care_home,
        Variables.non_res,
    ]

    data_source_columns = [
        IndCQC.ascwds_filled_posts_dedup_clean,
        Columns.CARE_HOME_EMPLOYED,
        Columns.NON_RESIDENTIAL_EMPLOYED,
        IndCQC.people_directly_employed,
    ]
