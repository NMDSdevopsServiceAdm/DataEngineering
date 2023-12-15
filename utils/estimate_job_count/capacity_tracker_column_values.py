from dataclasses import dataclass

from utils.estimate_job_count.column_names import (
    PEOPLE_DIRECTLY_EMPLOYED,
    JOB_COUNT_UNFILTERED,
    JOB_COUNT,
    ESTIMATE_JOB_COUNT,
    ROLLING_AVERAGE_MODEL,
    EXTRAPOLATION_MODEL,
    CARE_HOME_MODEL,
    INTERPOLATION_MODEL,
    NON_RESIDENTIAL_MODEL,
)
from utils.estimate_job_count.capacity_tracker_column_names import (
    CARE_HOME_EMPLOYED,
    NON_RESIDENTIAL_EMPLOYED,
)

known: str = "known"
unknown: str = "unknown"

care_home_with_nursing: str = "Care home with nursing"
care_home_without_nursing: str = "Care home without nursing"
non_residential: str = "non-residential"

asc_wds: str = "ascwds"
capacity_tracker: str = "capacity_tracker"
pir: str = "pir"

care_home: str = "care_home"
non_res: str = "non_res"

average_prefix:str = "avg_"


@dataclass
class ResidualsRequired:
    models = [
        ESTIMATE_JOB_COUNT,
        JOB_COUNT,
        ROLLING_AVERAGE_MODEL,
        CARE_HOME_MODEL,
        EXTRAPOLATION_MODEL,
        INTERPOLATION_MODEL,
        NON_RESIDENTIAL_MODEL,
    ]

    services = [
        care_home,
        non_res,
    ]

    data_source_columns = [
        JOB_COUNT_UNFILTERED,
        CARE_HOME_EMPLOYED,
        NON_RESIDENTIAL_EMPLOYED,
        PEOPLE_DIRECTLY_EMPLOYED,
    ]
