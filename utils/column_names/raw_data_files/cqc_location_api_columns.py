from dataclasses import dataclass


@dataclass
class CqcLocationApiColumns:
    address_line_one: str = "postalAddressLine1"
    care_home: str = "careHome"
    ccg_code: str = "onspdCcgCode"
    ccg_name: str = "onspdCcgName"
    code: str = "code"
    constituancy: str = "constituency"
    contacts: str = "contacts"
    county: str = "postalAddressCounty"
    current_ratings: str = "currentRatings"
    date: str = "date"
    deregistration_date: str = "deregistrationDate"
    description: str = "description"
    dormancy: str = "dormancy"
    family_name: str = "personFamilyName"
    first_visit_date: str = "firstVisitDate"
    gac_service_types: str = "gacServiceTypes"
    given_name: str = "personGivenName"
    historic_ratings: str = "historicRatings"
    inspection_categories: str = "inspectionCategories"
    inspection_directorate: str = "inspectionDirectorate"
    key_question_ratings: str = "keyQuestionRatings"
    last_inspection: str = "lastInspection"
    last_report: str = "lastReport"
    latitude: str = "onspdLatitude"
    link_id: str = "linkId"
    local_authority: str = "localAuthority"
    location_id: str = "locationId"
    longitude: str = "onspdLongitude"
    name: str = "name"
    number_of_beds: str = "numberOfBeds"
    ods_code: str = "odsCode"
    organisation_id: str = "organisationId"
    organisation_type: str = "organisationType"
    overall: str = "overall"
    phone_number: str = "mainPhoneNumber"
    postcode: str = "postalCode"
    primary: str = "primary"
    provider_id: str = "providerId"
    publication_date: str = "publicationDate"
    rating: str = "rating"
    reason: str = "reason"
    region: str = "region"
    registration_date: str = "registrationDate"
    registration_status: str = "registrationStatus"
    regulated_activities: str = "regulatedActivities"
    related_location_id: str = "relatedLocationId"
    related_location_name: str = "relatedLocationName"
    relationships: str = "relationships"
    report_date: str = "reportDate"
    report_link_id: str = "reportLinkId"
    report_type: str = "reportType"
    report_uri: str = "reportUri"
    reports: str = "reports"
    roles: str = "personRoles"
    specialisms: str = "specialisms"
    title: str = "personTitle"
    town_or_city: str = "postalAddressTownCity"
    type: str = "type"
    uprn: str = "uprn"
    website: str = "website"


@dataclass
class NewCqcLocationApiColumns:
    also_known_as: str = "alsoKnownAs"
    brand_id: str = "brandId"
    brand_name: str = "brandName"
    care_home: str = "careHome"
    code: str = "code"
    constituency: str = "constituency"
    contacts: str = "contacts"
    combined_quality_rating: str = "combinedQualityRating"
    combined_quality_summary: str = "combinedQualitySummary"
    current_ratings: str = "currentRatings"
    date: str = "date"
    deregistrationDate: str = "deregistrationDate"
    description: str = "description"
    documentType: str = "documentType"
    documentUri: str = "documentUri"
    dormancy: str = "dormancy"
    dormancyEndDate: str = "dormancyEndDate"
    dormancyStartDate: str = "dormancyStartDate"
    endDate: str = "endDate"
    firstVisitDate: str = "firstVisitDate"
    gacServiceTypes: str = "gacServiceTypes"
    historicRatings: str = "historicRatings"
    inspectionAreaId: str = "inspectionAreaId"
    inspectionAreaName: str = "inspectionAreaName"
    inspectionAreas: str = "inspectionAreas"
    inspectionAreaType: str = "inspectionAreaType"
    inspectionCategories: str = "inspectionCategories"
    inspectionDirectorate: str = "inspectionDirectorate"
    inspectionId: str = "inspectionId"
    key_question_ratings: str = "keyQuestionRatings"
    lastInspection: str = "lastInspection"
    lastReport: str = "lastReport"
    linkId: str = "linkId"
    localAuthority: str = "localAuthority"
    locationId: str = "locationId"
    locationTypes: str = "locationTypes"
    mainPhoneNumber: str = "mainPhoneNumber"
    name: str = "name"
    numberOfBeds: str = "numberOfBeds"
    odsCcgCode: str = "odsCcgCode"
    odsCcgName: str = "odsCcgName"
    odsCode: str = "odsCode"
    onspdCcgCode: str = "onspdCcgCode"
    onspdCcgName: str = "onspdCcgName"
    onspdIcbCode: str = "onspdIcbCode"
    onspdIcbName: str = "onspdIcbName"
    onspdLatitude: str = "onspdLatitude"
    onspdLongitude: str = "onspdLongitude"
    organisation_id: str = "organisationId"
    organisationType: str = "organisationType"
    overall: str = "overall"
    personFamilyName: str = "personFamilyName"
    personGivenName: str = "personGivenName"
    personRoles: str = "personRoles"
    personTitle: str = "personTitle"
    postalAddressCounty: str = "postalAddressCounty"
    postalAddressLine1: str = "postalAddressLine1"
    postalAddressLine2: str = "postalAddressLine2"
    postalAddressTownCity: str = "postalAddressTownCity"
    postalCode: str = "postalCode"
    primary: str = "primary"
    providerId: str = "providerId"
    providerInspectionAreas: str = "providerInspectionAreas"
    publicationDate: str = "publicationDate"
    rating: str = "rating"
    reason: str = "reason"
    region: str = "region"
    registeredManagerAbsentDate: str = "registeredManagerAbsentDate"
    registrationDate: str = "registrationDate"
    registrationStatus: str = "registrationStatus"
    regulatedActivities: str = "regulatedActivities"
    relatedDocuments: str = "relatedDocuments"
    relatedLocationId: str = "relatedLocationId"
    relatedLocationName: str = "relatedLocationName"
    relationships: str = "relationships"
    report_date: str = "reportDate"
    report_link_id: str = "reportLinkId"
    reportType: str = "reportType"
    reportUri: str = "reportUri"
    reports: str = "reports"
    serviceRatings: str = "serviceRatings"
    specialisms: str = "specialisms"
    status: str = "status"
    summary: str = "summary"
    supersededBy: str = "supersededBy"
    type: str = "type"
    unpublishedReports: str = "unpublishedReports"
    uprn: str = "uprn"
    use_of_resources: str = "useOfResources"
    useOfResourcesRating: str = "useOfResourcesRating"
    useOfResourcesSummary: str = "useOfResourcesSummary"
    website: str = "website"
