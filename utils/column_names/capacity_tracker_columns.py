from dataclasses import dataclass


@dataclass
class CapacityTrackerColumns:
    cqc_id: str = "CQC_ID"
    nurses_employed: str = "Nurses_Employed"
    care_workers_employed: str = "Care_Workers_Employed"
    non_care_workers_employed: str = "Non_Care_Workers_Employed"
    agency_nurses_employed: str = "Agency_Nurses_Employed"
    agency_care_workers_employed: str = "Agency_Care_Workers_Employed"
    agency_non_care_workers_employed: str = "Agency_Non_Care_Workers_Employed"
    cqc_care_workers_employed: str = "CQC_Care_Workers_Employed"
    care_home_employed: str = "care_home_employed"
    non_residential_employed: str = "non_residential_employed"
    description_of_changes: str = "description_of_changes"
    value: str = "value"
    id: str = "id"
    run_timestamp: str = "run_timestamp"
    capacity_tracker_care_homes_snapshot_date: str = (
        "capacity_tracker_care_homes_snapshot_date"
    )
    capacity_tracker_non_residential_snapshot_date: str = (
        "capacity_tracker_non_residential_snapshot_date"
    )
