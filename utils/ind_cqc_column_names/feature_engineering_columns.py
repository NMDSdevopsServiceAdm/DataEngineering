from dataclasses import dataclass


@dataclass
class CareHomeFeatureEngineeringColumns:
    carehome: str = "carehome"
    features: str = "features"
    job_count: str = "job_count"
    locationid: str = "locationid"
    number_of_beds: str = "number_of_beds"
    ons_region: str = "ons_region"
    partition_0: str = "partition_0"
    people_directly_employed: str = "people_directly_employed"
    snapshot_date: str = "snapshot_date"
    snapshot_day: str = "snapshot_day"
    snapshot_month: str = "snapshot_month"
    snapshot_year: str = "snapshot_year"



@dataclass
class NonResidentialFeatureEngineeringColumns:
    carehome: str = "carehome"
    features: str = "features"
    job_count: str = "job_count"
    locationid: str = "locationid"
    number_of_beds: str = "number_of_beds"
    ons_region: str = "ons_region"
    partition_0: str = "partition_0"
    people_directly_employed: str = "people_directly_employed"
    snapshot_date: str = "snapshot_date"
    snapshot_day: str = "snapshot_day"
    snapshot_month: str = "snapshot_month"
    snapshot_year: str = "snapshot_year"
