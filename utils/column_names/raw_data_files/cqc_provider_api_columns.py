from dataclasses import dataclass


@dataclass
class CqcProviderApiColumns:
    address_line_one: str = "postalAddressLine1"
    companies_house_number: str = "companiesHouseNumber"
    constituency: str = "constituency"
    county: str = "postalAddressCounty"
    deregistration_date: str = "deregistrationDate"
    inspection_directorate: str = "inspectionDirectorate"
    latitude: str = "onspdLatitude"
    local_authority: str = "localAuthority"
    location_ids: str = "locationIds"
    longitude: str = "onspdLongitude"
    name: str = "name"
    organisation_type: str = "organisationType"
    ownership_type: str = "ownershipType"
    phone_number: str = "mainPhoneNumber"
    postcode: str = "postalCode"
    provider_id: str = "providerId"
    region: str = "region"
    registration_date: str = "registrationDate"
    registration_status: str = "registrationStatus"
    town_or_city: str = "postalAddressTownCity"
    type: str = "type"
    uprn: str = "uprn"
