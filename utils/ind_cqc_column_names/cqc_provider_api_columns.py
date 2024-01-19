from dataclasses import dataclass


@dataclass
class CqcProviderApiColumns:
    provider_id: str = "providerId"
    location_ids: str = "locationIds"
    organisation_type: str = "organisationType"
    ownership_type: str = "ownershipType"
    type: str = "type"
    uprn: str = "uprn"
    name: str = "name"
    registration_status: str = "registrationStatus"
    registration_date: str = "registrationDate"
    deregistration_date: str = "deregistrationDate"
    address_line_one: str = "postalAddressLine1"
    town_or_city: str = "postalAddressTownCity"
    county: str = "postalAddressCounty"
    region: str = "region"
    postcode: str = "postalCode"
    latitude: str = "onspdLatitude"
    longitude: str = "onspdLongitude"
    phone_number: str = "mainPhoneNumber"
    companies_house_number: str = "companiesHouseNumber"
    inspection_directorate: str = "inspectionDirectorate"
    constituency: str = "constituency"
    local_authority: str = "localAuthority"
