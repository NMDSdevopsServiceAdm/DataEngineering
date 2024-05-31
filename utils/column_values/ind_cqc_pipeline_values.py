from dataclasses import dataclass

from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCQC,
)


@dataclass
class ASCWDSFilledPostsSource:
    """The possible values of the ASCWDS filled posts source column in the independent CQC estimates pipeline"""

    column_name: str = IndCQC.ascwds_filled_posts_source

    worker_records_and_total_staff: str = "worker records and total staff were the same"
    only_total_staff: str = "only totalstaff was provided"
    only_worker_records: str = "only wkrrecs was provided"
    average_of_total_staff_and_worker_records: str = (
        "average of total staff and worker records as both were similar"
    )


@dataclass
class EstimateFilledPostsSource:
    """The possible values of the estimate filled posts source column in the independent CQC estimates pipeline"""

    column_name: str = IndCQC.estimate_filled_posts_source

    rolling_average_model: str = "rolling_average_model"
    care_home_model: str = "care_home_model"
    interpolation_model: str = "interpolation_model"
    extrapolation_model: str = "extrapolation_model"
    ascwds_filled_posts_clean_deduplicated: str = (
        "ascwds_filled_posts_clean_deduplicated"
    )
    non_res_with_pir_model: str = "non_res_with_pir_model"
