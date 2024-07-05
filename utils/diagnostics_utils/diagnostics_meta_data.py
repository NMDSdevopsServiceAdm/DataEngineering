from dataclasses import dataclass

from utils.column_names.ind_cqc_pipeline_columns import (
    PartitionKeys as Keys,
    IndCqcColumns as IndCQC,
)
from utils.column_names.capacity_tracker_columns import CapacityTrackerColumns as CT
from utils.column_values.categorical_column_values import CareHome


@dataclass
class Variables:
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
        CareHome.care_home,
        CareHome.not_care_home,
    ]

    data_source_columns = [
        IndCQC.ascwds_filled_posts_dedup_clean,
        CT.care_home_employed,
        CT.non_residential_employed,
        IndCQC.people_directly_employed,
    ]
