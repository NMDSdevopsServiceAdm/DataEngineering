from dataclasses import dataclass


@dataclass
class CareHomeFeatureEngineeringColumns:
    locationid: str = "locationid"
    snapshot_date: str = "snapshot_date"
    ons_region: str = "ons_region"
    number_of_beds: str = "number_of_beds"
    people_directly_employed: str = "people_directly_employed"
    carehome: str = "carehome"
    features: str = "features"
    job_count: str = "job_count"
    partition_0: str = "partition_0"
    snapshot_year: str = "snapshot_year"
    snapshot_month: str = "snapshot_month"
    snapshot_day: str = "snapshot_day"


@dataclass
class NonResidentialFeatureEngineeringColumns:
    locationid: str = "locationid"
    snapshot_date: str = "snapshot_date"
    ons_region: str = "ons_region"
    number_of_beds: str = "number_of_beds"
    people_directly_employed: str = "people_directly_employed"
    carehome: str = "carehome"
    features: str = "features"
    job_count: str = "job_count"
    partition_0: str = "partition_0"
    snapshot_year: str = "snapshot_year"
    snapshot_month: str = "snapshot_month"
    snapshot_day: str = "snapshot_day"
