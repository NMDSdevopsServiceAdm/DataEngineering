from polars import Schema, String, Int64, Struct, List


raw_providers_schema = Schema(
    [
        ("providerId", String()),
        ("locationIds", List(String())),
        ("organisationType", String()),
        ("ownershipType", String()),
        ("type", String()),
        ("name", String()),
        ("brandId", String()),
        ("brandName", String()),
        ("odsCode", String()),
        ("registrationStatus", String()),
        ("registrationDate", String()),
        ("companiesHouseNumber", String()),
        ("charityNumber", String()),
        ("website", String()),
        ("postalAddressLine1", String()),
        ("postalAddressLine2", String()),
        ("postalAddressTownCity", String()),
        ("postalAddressCounty", String()),
        ("region", String()),
        ("postalCode", String()),
        ("alsoKnownAs", String()),
        ("deregistrationDate", String()),
        ("uprn", String()),
        ("onspdLatitude", String()),
        ("onspdLongitude", String()),
        ("onspdIcbCode", String()),
        ("onspdIcbName", String()),
        ("mainPhoneNumber", String()),
        ("inspectionDirectorate", String()),
        ("constituency", String()),
        ("localAuthority", String()),
        ("lastInspection", Struct({"date": String()})),
        ("lastReport", Struct({"publicationDate": String()})),
        (
            "contacts",
            List(
                Struct(
                    {
                        "personTitle": String(),
                        "personGivenName": String(),
                        "personFamilyName": String(),
                        "personRoles": List(String()),
                    }
                )
            ),
        ),
        (
            "relationships",
            List(
                Struct(
                    {
                        "relatedProviderId": String(),
                        "relatedProviderName": String(),
                        "type": String(),
                        "reason": String(),
                    }
                )
            ),
        ),
        (
            "regulatedActivities",
            List(
                Struct(
                    {
                        "name": String(),
                        "code": String(),
                        "nominatedIndividual": Struct(
                            {
                                "personTitle": String(),
                                "personGivenName": String(),
                                "personFamilyName": String(),
                            }
                        ),
                    }
                )
            ),
        ),
        (
            "inspectionCategories",
            List(Struct({"code": String(), "primary": String(), "name": String()})),
        ),
        (
            "inspectionAreas",
            List(
                Struct(
                    {
                        "inspectionAreaId": String(),
                        "inspectionAreaName": String(),
                        "inspectionAreaType": String(),
                        "status": String(),
                        "endDate": String(),
                        "supersededBy": List(String()),
                    }
                )
            ),
        ),
        (
            "currentRatings",
            Struct(
                {
                    "overall": Struct(
                        {
                            "rating": String(),
                            "reportDate": String(),
                            "reportLinkId": String(),
                            "useOfResources": Struct(
                                {
                                    "useOfResourcesSummary": String(),
                                    "useOfResourcesRating": String(),
                                    "combinedQualitySummary": String(),
                                    "combinedQualityRating": String(),
                                    "reportDate": String(),
                                    "reportLinkId": String(),
                                }
                            ),
                            "keyQuestionRatings": List(
                                Struct(
                                    {
                                        "name": String(),
                                        "rating": String(),
                                        "reportDate": String(),
                                        "organisationId": String(),
                                        "reportLinkId": String(),
                                    }
                                )
                            ),
                        }
                    ),
                    "serviceRatings": List(
                        Struct(
                            {
                                "name": String(),
                                "rating": String(),
                                "reportDate": String(),
                                "organisationId": String(),
                                "reportLinkId": String(),
                                "keyQuestionRatings": List(
                                    Struct({"name": String(), "rating": String()})
                                ),
                            }
                        )
                    ),
                }
            ),
        ),
        (
            "historicRatings",
            List(
                Struct(
                    {
                        "reportDate": String(),
                        "reportLinkId": String(),
                        "organisationId": String(),
                        "overall": Struct(
                            {
                                "rating": String(),
                                "useOfResources": Struct(
                                    {
                                        "useOfResourcesSummary": String(),
                                        "useOfResourcesRating": String(),
                                        "combinedQualitySummary": String(),
                                        "combinedQualityRating": String(),
                                    }
                                ),
                                "keyQuestionRatings": List(
                                    Struct({"name": String(), "rating": String()})
                                ),
                            }
                        ),
                        "serviceRatings": List(
                            Struct(
                                {
                                    "name": String(),
                                    "rating": String(),
                                    "keyQuestionRatings": List(
                                        Struct({"name": String(), "rating": String()})
                                    ),
                                }
                            )
                        ),
                    }
                )
            ),
        ),
        (
            "reports",
            List(
                Struct(
                    {
                        "linkId": String(),
                        "reportDate": String(),
                        "firstVisitDate": String(),
                        "reportUri": String(),
                        "reportType": String(),
                        "inspectionLocations": List(Struct({"locationId": String()})),
                        "relatedDocuments": List(
                            Struct({"documentUri": String(), "documentType": String()})
                        ),
                    }
                )
            ),
        ),
        ("unpublishedReports", List(Struct({"firstVisitDate": String()}))),
    ]
)
