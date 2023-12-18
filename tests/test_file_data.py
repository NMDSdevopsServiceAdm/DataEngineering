from dataclasses import dataclass

from utils.diagnostics_utils.diagnostics_meta_data import (
    CategoricalVariables as Values,
)

@dataclass
class CreateJobEstimatesDiagnosticsData:
    # fmt: off
    estimate_jobs_rows = [
        ("location_1", 40.0, 40.0, Values.care_home_with_nursing, 60.9, 23.4, 45.1, None, None, 40.0, 45,),
    ]
    capacity_tracker_care_home_rows = [
        ("location_1", 8.0, 12.0, 15.0, 1.0, 3.0, 2.0),
    ]
    capacity_tracker_non_residential_rows = [
        ("location_2", 67.0),
    ]
    prepare_capacity_tracker_care_home_rows = [
        ("location_1", Values.care_home_with_nursing, 8.0, 12.0, 15.0, 1.0, 3.0, 2.0, None,),
        ("location_2", Values.non_residential,  None, None, None, None, None, None, 30.0,),
    ]
    prepare_capacity_tracker_non_residential_rows = [
        ("location_1", Values.care_home_with_nursing, 8.0, 12.0, 15.0, 1.0, 3.0, 2.0, None,),
        ("location_2", Values.non_residential, None, None, None, None, None, None, 75.0,),
    ]
    calculate_residuals_rows = [
        ("location_2", 40.0, 40.0, Values.non_residential, 60.9, 23.4, 45.1, None, None, 40.0, 40, None, 40.0,),
        ("location_3", 40.0, 40.0, Values.non_residential, 60.9, 23.4, 45.1, None, None, 40.0, 45, 41.0, None,),
        ("location_4", None, None, Values.non_residential, 60.9, 23.4, None, None, None, 60.0, 45, None, None,),
        ("location_5", None, None, Values.care_home_with_nursing, 60.9, 23.4, None, None, None, 60.9, None, 50.0, None,),
    ]
    run_residuals_rows = [
        ("location_1", None, None, Values.care_home_with_nursing, 60.9, 23.4, None, None, None, 60.9, None, None, None,),
        ("location_2", 40.0, 40.0, Values.non_residential, 60.9, 23.4, 45.1, None, None, 40.0, None, None, 40.0,),
        ("location_3", 40.0, 40.0, Values.care_home_without_nursing, 60.9, 23.4, 45.1, None, None, 40.0, 45, 41.0, None,),
        ("location_4", None, None, Values.care_home_with_nursing, 60.9, 23.4, None, None, None, 60.9, 45, None, None,),
        ("location_5", None, None, Values.care_home_with_nursing, 60.9, 23.4, None, None, None, 60.9, None, 50.0, None,),
        ("location_6", None, None, Values.care_home_with_nursing, 60.9, 23.4, None, None, None, 60.9, 45, 50.0, None,),
        ("location_7", None, None, Values.non_residential, 60.9, None, None, None, 40.0, 60.9, 45, None, None,),
        ("location_8", None, None, Values.non_residential, 60.9, None, None, None, 40.0, 60.9, None, None, 40.0,),
        ("location_9", None, None, Values.non_residential, 60.9, None, None, None, 40.0, 60.9, 45, None, 40.0,),
        ("location_10", None, None, Values.care_home_with_nursing, 60.9, 23.4, None, None, None, 60.9, None, None, None,),
    ]
    residuals_rows = [
        ("location_1", 0.0, 0.0,),
        ("location_2", -1.0, 0.0,),
        ("location_3", 3.0, 0.0,),
        ("location_4", None, 0.0,),
        ("location_5", 10.5, 0.0,),
        ("location_6", -2.5, 0.0,),
        ("location_7", None, None,),
        ("location_8", None, None,),
    ]
    add_timestamps_rows = [
        ("location_1", 0.0, 0.0,),
        ("location_2", -1.0, 0.0,),
    ]
    # fmt: on
    description_of_change: str = "test"
    run_timestamp:str = "12/24/2018, 04:59:31"