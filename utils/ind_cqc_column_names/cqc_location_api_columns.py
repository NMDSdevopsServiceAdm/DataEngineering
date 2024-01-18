from dataclasses import dataclass

@dataclass
class CqcCareDirectoryColumns:
    location_id: str = "locationid"
    provider_id: str = "providerid"
    organisation_type:str = "organisationType"
    type: str = "type"
    name: str = "name"