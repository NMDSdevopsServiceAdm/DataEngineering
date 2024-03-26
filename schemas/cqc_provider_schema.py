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
        StructField("alsoKnownAs", StringType(), True),
        StructField("brandId", StringType(), True),
        StructField("brandName", StringType(), True),
        StructField("charityNumber", StringType(), True),
        StructField("companiesHouseNumber", StringType(), True),
        StructField("constituency", StringType(), True),
        StructField(
            "contacts",
            ArrayType(
                StructType(
                    [
                        StructField("personFamilyName", StringType(), True),
                        StructField("personGivenName", StringType(), True),
                        StructField("personRoles", ArrayType(StringType(), True), True),
                        StructField("personTitle", StringType(), True),
                    ]
                ),
                True,
            ),
            True,
        ),
        StructField(
            "currentRatings",
            StructType(
                [
                    StructField(
                        "overall",
                        StructType(
                            [
                                StructField(
                                    "keyQuestionRatings",
                                    ArrayType(
                                        StructType(
                                            [
                                                StructField("name", StringType(), True),
                                                StructField(
                                                    "organisationId", StringType(), True
                                                ),
                                                StructField(
                                                    "rating", StringType(), True
                                                ),
                                                StructField(
                                                    "reportDate", StringType(), True
                                                ),
                                                StructField(
                                                    "reportLinkId", StringType(), True
                                                ),
                                            ]
                                        ),
                                        True,
                                    ),
                                    True,
                                ),
                                StructField("rating", StringType(), True),
                                StructField("reportDate", StringType(), True),
                                StructField("reportLinkId", StringType(), True),
                                StructField(
                                    "useOfResources",
                                    StructType(
                                        [
                                            StructField(
                                                "combinedQualityRating",
                                                StringType(),
                                                True,
                                            ),
                                            StructField(
                                                "combinedQualitySummary",
                                                StringType(),
                                                True,
                                            ),
                                            StructField(
                                                "reportDate", StringType(), True
                                            ),
                                            StructField(
                                                "reportLinkId", StringType(), True
                                            ),
                                            StructField(
                                                "useOfResourcesRating",
                                                StringType(),
                                                True,
                                            ),
                                            StructField(
                                                "useOfResourcesSummary",
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
                        "serviceRatings",
                        ArrayType(
                            StructType(
                                [
                                    StructField(
                                        "keyQuestionRatings",
                                        ArrayType(
                                            StructType(
                                                [
                                                    StructField(
                                                        "name", StringType(), True
                                                    ),
                                                    StructField(
                                                        "rating", StringType(), True
                                                    ),
                                                ]
                                            ),
                                            True,
                                        ),
                                        True,
                                    ),
                                    StructField("name", StringType(), True),
                                    StructField("organisationId", StringType(), True),
                                    StructField("rating", StringType(), True),
                                    StructField("reportDate", StringType(), True),
                                    StructField("reportLinkId", StringType(), True),
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
        StructField("deregistrationDate", StringType(), True),
        StructField(
            "historicRatings",
            ArrayType(
                StructType(
                    [
                        StructField("organisationId", StringType(), True),
                        StructField(
                            "overall",
                            StructType(
                                [
                                    StructField(
                                        "keyQuestionRatings",
                                        ArrayType(
                                            StructType(
                                                [
                                                    StructField(
                                                        "name", StringType(), True
                                                    ),
                                                    StructField(
                                                        "rating", StringType(), True
                                                    ),
                                                ]
                                            ),
                                            True,
                                        ),
                                        True,
                                    ),
                                    StructField("rating", StringType(), True),
                                    StructField(
                                        "useOfResources",
                                        StructType(
                                            [
                                                StructField(
                                                    "combinedQualityRating",
                                                    StringType(),
                                                    True,
                                                ),
                                                StructField(
                                                    "combinedQualitySummary",
                                                    StringType(),
                                                    True,
                                                ),
                                                StructField(
                                                    "useOfResourcesRating",
                                                    StringType(),
                                                    True,
                                                ),
                                                StructField(
                                                    "useOfResourcesSummary",
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
                        StructField("reportDate", StringType(), True),
                        StructField("reportLinkId", StringType(), True),
                        StructField(
                            "serviceRatings",
                            ArrayType(
                                StructType(
                                    [
                                        StructField(
                                            "keyQuestionRatings",
                                            ArrayType(
                                                StructType(
                                                    [
                                                        StructField(
                                                            "name", StringType(), True
                                                        ),
                                                        StructField(
                                                            "rating", StringType(), True
                                                        ),
                                                    ]
                                                ),
                                                True,
                                            ),
                                            True,
                                        ),
                                        StructField("name", StringType(), True),
                                        StructField("rating", StringType(), True),
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
            "inspectionAreas",
            ArrayType(
                StructType(
                    [
                        StructField("endDate", StringType(), True),
                        StructField("inspectionAreaId", StringType(), True),
                        StructField("inspectionAreaName", StringType(), True),
                        StructField("inspectionAreaType", StringType(), True),
                        StructField("status", StringType(), True),
                        StructField(
                            "supersededBy", ArrayType(StringType(), True), True
                        ),
                    ]
                ),
                True,
            ),
            True,
        ),
        StructField(
            "inspectionCategories",
            ArrayType(
                StructType(
                    [
                        StructField("code", StringType(), True),
                        StructField("name", StringType(), True),
                        StructField("primary", StringType(), True),
                    ]
                ),
                True,
            ),
            True,
        ),
        StructField("inspectionDirectorate", StringType(), True),
        StructField(
            "lastInspection",
            StructType([StructField("date", StringType(), True)]),
            True,
        ),
        StructField(
            "lastReport",
            StructType([StructField("publicationDate", StringType(), True)]),
            True,
        ),
        StructField("localAuthority", StringType(), True),
        StructField("locationIds", ArrayType(StringType(), True), True),
        StructField("mainPhoneNumber", StringType(), True),
        StructField("name", StringType(), True),
        StructField("odsCode", StringType(), True),
        StructField("onspdIcbCode", StringType(), True),
        StructField("onspdIcbName", StringType(), True),
        StructField("onspdLatitude", DoubleType(), True),
        StructField("onspdLongitude", DoubleType(), True),
        StructField("organisationType", StringType(), True),
        StructField("ownershipType", StringType(), True),
        StructField("postalAddressCounty", StringType(), True),
        StructField("postalAddressLine1", StringType(), True),
        StructField("postalAddressLine2", StringType(), True),
        StructField("postalAddressTownCity", StringType(), True),
        StructField("postalCode", StringType(), True),
        StructField("providerId", StringType(), True),
        StructField("region", StringType(), True),
        StructField("registrationDate", StringType(), True),
        StructField("registrationStatus", StringType(), True),
        StructField(
            "regulatedActivities",
            ArrayType(
                StructType(
                    [
                        StructField("code", StringType(), True),
                        StructField("name", StringType(), True),
                        StructField(
                            "nominatedIndividual",
                            StructType(
                                [
                                    StructField("personFamilyName", StringType(), True),
                                    StructField("personGivenName", StringType(), True),
                                    StructField("personTitle", StringType(), True),
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
            "relationships",
            ArrayType(
                StructType(
                    [
                        StructField("reason", StringType(), True),
                        StructField("relatedProviderId", StringType(), True),
                        StructField("relatedProviderName", StringType(), True),
                        StructField("type", StringType(), True),
                    ]
                ),
                True,
            ),
            True,
        ),
        StructField(
            "reports",
            ArrayType(
                StructType(
                    [
                        StructField("firstVisitDate", StringType(), True),
                        StructField(
                            "inspectionLocations",
                            ArrayType(
                                StructType(
                                    [StructField("locationId", StringType(), True)]
                                ),
                                True,
                            ),
                            True,
                        ),
                        StructField("linkId", StringType(), True),
                        StructField(
                            "relatedDocuments",
                            ArrayType(
                                StructType(
                                    [
                                        StructField("documentType", StringType(), True),
                                        StructField("documentUri", StringType(), True),
                                    ]
                                ),
                                True,
                            ),
                            True,
                        ),
                        StructField("reportDate", StringType(), True),
                        StructField("reportType", StringType(), True),
                        StructField("reportUri", StringType(), True),
                    ]
                ),
                True,
            ),
            True,
        ),
        StructField("type", StringType(), True),
        StructField(
            "unpublishedReports",
            ArrayType(
                StructType([StructField("firstVisitDate", StringType(), True)]), True
            ),
            True,
        ),
        StructField("uprn", StringType(), True),
        StructField("website", StringType(), True),
    ]
)
