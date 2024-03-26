from pyspark.sql.types import (
    StructField,
    StructType,
    StringType,
    ArrayType,
    FloatType,
    DoubleType,
)

from utils.column_names.raw_data_files.cqc_provider_api_columns import (
    CqcProviderApiColumns as OldColNames,
)
from utils.column_names.raw_data_files.cqc_provider_api_columns import (
    NewCqcProviderApiColumns as NewColNames,
)

OLD_PROVIDER_SCHEMA = StructType(
    fields=[
        StructField(OldColNames.provider_id, StringType(), True),
        StructField(
            OldColNames.location_ids,
            ArrayType(
                StringType(),
            ),
        ),
        StructField(OldColNames.organisation_type, StringType(), True),
        StructField(OldColNames.ownership_type, StringType(), True),
        StructField(OldColNames.type, StringType(), True),
        StructField(OldColNames.uprn, StringType(), True),
        StructField(OldColNames.name, StringType(), True),
        StructField(OldColNames.registration_status, StringType(), True),
        StructField(OldColNames.registration_date, StringType(), True),
        StructField(OldColNames.deregistration_date, StringType(), True),
        StructField(OldColNames.address_line_one, StringType(), True),
        StructField(OldColNames.town_or_city, StringType(), True),
        StructField(OldColNames.county, StringType(), True),
        StructField(OldColNames.region, StringType(), True),
        StructField(OldColNames.postcode, StringType(), True),
        StructField(OldColNames.latitude, FloatType(), True),
        StructField(OldColNames.longitude, FloatType(), True),
        StructField(OldColNames.phone_number, StringType(), True),
        StructField(OldColNames.companies_house_number, StringType(), True),
        StructField(OldColNames.inspection_directorate, StringType(), True),
        StructField(OldColNames.constituency, StringType(), True),
        StructField(OldColNames.local_authority, StringType(), True),
    ]
)

NEW_PROVIDER_SCHEMA = StructType(
    [
        StructField(NewColNames.alsoKnownAs, StringType(), True),
        StructField(NewColNames.brandId, StringType(), True),
        StructField(NewColNames.brandName, StringType(), True),
        StructField(NewColNames.charityNumber, StringType(), True),
        StructField(NewColNames.companiesHouseNumber, StringType(), True),
        StructField(NewColNames.constituency, StringType(), True),
        StructField(
            NewColNames.contacts,
            ArrayType(
                StructType(
                    [
                        StructField(NewColNames.personFamilyName, StringType(), True),
                        StructField(NewColNames.personGivenName, StringType(), True),
                        StructField(
                            NewColNames.personRoles, ArrayType(StringType(), True), True
                        ),
                        StructField(NewColNames.personTitle, StringType(), True),
                    ]
                ),
                True,
            ),
            True,
        ),
        StructField(
            NewColNames.currentRatings,
            StructType(
                [
                    StructField(
                        NewColNames.overall,
                        StructType(
                            [
                                StructField(
                                    NewColNames.keyQuestionRatings,
                                    ArrayType(
                                        StructType(
                                            [
                                                StructField(
                                                    NewColNames.name, StringType(), True
                                                ),
                                                StructField(
                                                    NewColNames.organisationId,
                                                    StringType(),
                                                    True,
                                                ),
                                                StructField(
                                                    NewColNames.rating,
                                                    StringType(),
                                                    True,
                                                ),
                                                StructField(
                                                    NewColNames.reportDate,
                                                    StringType(),
                                                    True,
                                                ),
                                                StructField(
                                                    NewColNames.reportLinkId,
                                                    StringType(),
                                                    True,
                                                ),
                                            ]
                                        ),
                                        True,
                                    ),
                                    True,
                                ),
                                StructField(NewColNames.rating, StringType(), True),
                                StructField(NewColNames.reportDate, StringType(), True),
                                StructField(
                                    NewColNames.reportLinkId, StringType(), True
                                ),
                                StructField(
                                    NewColNames.useOfResources,
                                    StructType(
                                        [
                                            StructField(
                                                NewColNames.combinedQualityRating,
                                                StringType(),
                                                True,
                                            ),
                                            StructField(
                                                NewColNames.combinedQualitySummary,
                                                StringType(),
                                                True,
                                            ),
                                            StructField(
                                                NewColNames.reportDate,
                                                StringType(),
                                                True,
                                            ),
                                            StructField(
                                                NewColNames.reportLinkId,
                                                StringType(),
                                                True,
                                            ),
                                            StructField(
                                                NewColNames.useOfResourcesRating,
                                                StringType(),
                                                True,
                                            ),
                                            StructField(
                                                NewColNames.useOfResourcesSummary,
                                                StringType(),
                                                True,
                                            ),
                                        ]
                                    ),
                                    True,
                                ),
                            ]
                        ),
                        True,
                    ),
                    StructField(
                        NewColNames.serviceRatings,
                        ArrayType(
                            StructType(
                                [
                                    StructField(
                                        NewColNames.keyQuestionRatings,
                                        ArrayType(
                                            StructType(
                                                [
                                                    StructField(
                                                        NewColNames.name,
                                                        StringType(),
                                                        True,
                                                    ),
                                                    StructField(
                                                        NewColNames.rating,
                                                        StringType(),
                                                        True,
                                                    ),
                                                ]
                                            ),
                                            True,
                                        ),
                                        True,
                                    ),
                                    StructField(NewColNames.name, StringType(), True),
                                    StructField(
                                        NewColNames.organisationId, StringType(), True
                                    ),
                                    StructField(NewColNames.rating, StringType(), True),
                                    StructField(
                                        NewColNames.reportDate, StringType(), True
                                    ),
                                    StructField(
                                        NewColNames.reportLinkId, StringType(), True
                                    ),
                                ]
                            ),
                            True,
                        ),
                        True,
                    ),
                ]
            ),
            True,
        ),
        StructField(NewColNames.deregistrationDate, StringType(), True),
        StructField(
            NewColNames.historicRatings,
            ArrayType(
                StructType(
                    [
                        StructField(NewColNames.organisationId, StringType(), True),
                        StructField(
                            NewColNames.overall,
                            StructType(
                                [
                                    StructField(
                                        NewColNames.keyQuestionRatings,
                                        ArrayType(
                                            StructType(
                                                [
                                                    StructField(
                                                        NewColNames.name,
                                                        StringType(),
                                                        True,
                                                    ),
                                                    StructField(
                                                        NewColNames.rating,
                                                        StringType(),
                                                        True,
                                                    ),
                                                ]
                                            ),
                                            True,
                                        ),
                                        True,
                                    ),
                                    StructField(NewColNames.rating, StringType(), True),
                                    StructField(
                                        NewColNames.useOfResources,
                                        StructType(
                                            [
                                                StructField(
                                                    NewColNames.combinedQualityRating,
                                                    StringType(),
                                                    True,
                                                ),
                                                StructField(
                                                    NewColNames.combinedQualitySummary,
                                                    StringType(),
                                                    True,
                                                ),
                                                StructField(
                                                    NewColNames.useOfResourcesRating,
                                                    StringType(),
                                                    True,
                                                ),
                                                StructField(
                                                    NewColNames.useOfResourcesSummary,
                                                    StringType(),
                                                    True,
                                                ),
                                            ]
                                        ),
                                        True,
                                    ),
                                ]
                            ),
                            True,
                        ),
                        StructField(NewColNames.reportDate, StringType(), True),
                        StructField(NewColNames.reportLinkId, StringType(), True),
                        StructField(
                            NewColNames.serviceRatings,
                            ArrayType(
                                StructType(
                                    [
                                        StructField(
                                            NewColNames.keyQuestionRatings,
                                            ArrayType(
                                                StructType(
                                                    [
                                                        StructField(
                                                            NewColNames.name,
                                                            StringType(),
                                                            True,
                                                        ),
                                                        StructField(
                                                            NewColNames.rating,
                                                            StringType(),
                                                            True,
                                                        ),
                                                    ]
                                                ),
                                                True,
                                            ),
                                            True,
                                        ),
                                        StructField(
                                            NewColNames.name, StringType(), True
                                        ),
                                        StructField(
                                            NewColNames.rating, StringType(), True
                                        ),
                                    ]
                                ),
                                True,
                            ),
                            True,
                        ),
                    ]
                ),
                True,
            ),
            True,
        ),
        StructField(
            NewColNames.inspectionAreas,
            ArrayType(
                StructType(
                    [
                        StructField(NewColNames.endDate, StringType(), True),
                        StructField(NewColNames.inspectionAreaId, StringType(), True),
                        StructField(NewColNames.inspectionAreaName, StringType(), True),
                        StructField(NewColNames.inspectionAreaType, StringType(), True),
                        StructField(NewColNames.status, StringType(), True),
                        StructField(
                            NewColNames.supersededBy,
                            ArrayType(StringType(), True),
                            True,
                        ),
                    ]
                ),
                True,
            ),
            True,
        ),
        StructField(
            NewColNames.inspectionCategories,
            ArrayType(
                StructType(
                    [
                        StructField(NewColNames.code, StringType(), True),
                        StructField(NewColNames.name, StringType(), True),
                        StructField(NewColNames.primary, StringType(), True),
                    ]
                ),
                True,
            ),
            True,
        ),
        StructField(NewColNames.inspectionDirectorate, StringType(), True),
        StructField(
            NewColNames.lastInspection,
            StructType([StructField(NewColNames.date, StringType(), True)]),
            True,
        ),
        StructField(
            NewColNames.lastReport,
            StructType([StructField(NewColNames.publicationDate, StringType(), True)]),
            True,
        ),
        StructField(NewColNames.localAuthority, StringType(), True),
        StructField(NewColNames.locationIds, ArrayType(StringType(), True), True),
        StructField(NewColNames.mainPhoneNumber, StringType(), True),
        StructField(NewColNames.name, StringType(), True),
        StructField(NewColNames.odsCode, StringType(), True),
        StructField(NewColNames.onspdIcbCode, StringType(), True),
        StructField(NewColNames.onspdIcbName, StringType(), True),
        StructField(NewColNames.onspdLatitude, DoubleType(), True),
        StructField(NewColNames.onspdLongitude, DoubleType(), True),
        StructField(NewColNames.organisationType, StringType(), True),
        StructField(NewColNames.ownershipType, StringType(), True),
        StructField(NewColNames.postalAddressCounty, StringType(), True),
        StructField(NewColNames.postalAddressLine1, StringType(), True),
        StructField(NewColNames.postalAddressLine2, StringType(), True),
        StructField(NewColNames.postalAddressTownCity, StringType(), True),
        StructField(NewColNames.postalCode, StringType(), True),
        StructField(NewColNames.providerId, StringType(), True),
        StructField(NewColNames.region, StringType(), True),
        StructField(NewColNames.registrationDate, StringType(), True),
        StructField(NewColNames.registrationStatus, StringType(), True),
        StructField(
            NewColNames.regulatedActivities,
            ArrayType(
                StructType(
                    [
                        StructField(NewColNames.code, StringType(), True),
                        StructField(NewColNames.name, StringType(), True),
                        StructField(
                            NewColNames.nominatedIndividual,
                            StructType(
                                [
                                    StructField(
                                        NewColNames.personFamilyName, StringType(), True
                                    ),
                                    StructField(
                                        NewColNames.personGivenName, StringType(), True
                                    ),
                                    StructField(
                                        NewColNames.personTitle, StringType(), True
                                    ),
                                ]
                            ),
                            True,
                        ),
                    ]
                ),
                True,
            ),
            True,
        ),
        StructField(
            NewColNames.relationships,
            ArrayType(
                StructType(
                    [
                        StructField(NewColNames.reason, StringType(), True),
                        StructField(NewColNames.relatedProviderId, StringType(), True),
                        StructField(
                            NewColNames.relatedProviderName, StringType(), True
                        ),
                        StructField(NewColNames.type, StringType(), True),
                    ]
                ),
                True,
            ),
            True,
        ),
        StructField(
            NewColNames.reports,
            ArrayType(
                StructType(
                    [
                        StructField(NewColNames.firstVisitDate, StringType(), True),
                        StructField(
                            NewColNames.inspectionLocations,
                            ArrayType(
                                StructType(
                                    [
                                        StructField(
                                            NewColNames.locationId, StringType(), True
                                        )
                                    ]
                                ),
                                True,
                            ),
                            True,
                        ),
                        StructField(NewColNames.linkId, StringType(), True),
                        StructField(
                            NewColNames.relatedDocuments,
                            ArrayType(
                                StructType(
                                    [
                                        StructField(
                                            NewColNames.documentType, StringType(), True
                                        ),
                                        StructField(
                                            NewColNames.documentUri, StringType(), True
                                        ),
                                    ]
                                ),
                                True,
                            ),
                            True,
                        ),
                        StructField(NewColNames.reportDate, StringType(), True),
                        StructField(NewColNames.reportType, StringType(), True),
                        StructField(NewColNames.reportUri, StringType(), True),
                    ]
                ),
                True,
            ),
            True,
        ),
        StructField(NewColNames.type, StringType(), True),
        StructField(
            NewColNames.unpublishedReports,
            ArrayType(
                StructType(
                    [StructField(NewColNames.firstVisitDate, StringType(), True)]
                ),
                True,
            ),
            True,
        ),
        StructField(NewColNames.uprn, StringType(), True),
        StructField(NewColNames.website, StringType(), True),
    ]
)
