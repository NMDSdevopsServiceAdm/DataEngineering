from polars import Schema, String, Int32, Struct, List

raw_schema = Schema(
    [
        ("locationId", String()),
        ("providerId", String()),
        ("organisationType", String()),
        ("type", String()),
        ("name", String()),
        ("brandId", String()),
        ("brandName", String()),
        ("onspdCcgCode", String()),
        ("onspdCcgName", String()),
        ("odsCcgCode", String()),
        ("odsCcgName", String()),
        ("onspdIcbCode", String()),
        ("onspdIcbName", String()),
        ("odsCode", String()),
        ("registrationStatus", String()),
        ("registrationDate", String()),
        ("deregistrationDate", String()),
        ("dormancy", String()),
        ("dormancyStartDate", String()),
        ("dormancyEndDate", String()),
        ("alsoKnownAs", String()),
        ("onspdLatitude", String()),
        ("onspdLongitude", String()),
        ("careHome", String()),
        ("inspectionDirectorate", String()),
        ("website", String()),
        ("postalAddressLine1", String()),
        ("postalAddressLine2", String()),
        ("postalAddressTownCity", String()),
        ("postalAddressCounty", String()),
        ("region", String()),
        ("postalCode", String()),
        ("uprn", String()),
        ("mainPhoneNumber", String()),
        ("registeredManagerAbsentDate", String()),
        ("numberOfBeds", Int32()),
        ("constituency", String()),
        ("localAuthority", String()),
        ("lastInspection", Struct({"date": String()})),
        ("lastReport", Struct({"publicationDate": String()})),
        (
            "relationships",
            List(
                Struct(
                    {
                        "relatedLocationId": String(),
                        "relatedLocationName": String(),
                        "type": String(),
                        "reason": String(),
                    }
                )
            ),
        ),
        ("locationTypes", List(Struct({"type": String()}))),
        (
            "regulatedActivities",
            List(
                Struct(
                    {
                        "name": String(),
                        "code": String(),
                        "contacts": List(
                            Struct(
                                {
                                    "personFamilyName": String(),
                                    "personGivenName": String(),
                                    "personRoles": List(String()),
                                    "personTitle": String(),
                                    "col3": List(String()),
                                }
                            )
                        ),
                    }
                )
            ),
        ),
        ("gacServiceTypes", List(Struct({"name": String(), "description": String()}))),
        ("specialisms", List(Struct({"name": String()}))),
        (
            "inspectionCategories",
            List(Struct({"primary": String(), "code": String(), "name": String()})),
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
                        "supersededBy": List(String()),
                        "endDate": String(),
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
                            "organisationId": String(),
                            "rating": String(),
                            "reportDate": String(),
                            "reportLinkId": String(),
                            "useOfResources": Struct(
                                {
                                    "organisationId": String(),
                                    "summary": String(),
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
                        "overall": Struct(
                            {
                                "rating": String(),
                                "useOfResources": Struct(
                                    {
                                        "combinedQualityRating": String(),
                                        "combinedQualitySummary": String(),
                                        "useOfResourcesRating": String(),
                                        "useOfResourcesSummary": String(),
                                    }
                                ),
                                "keyQuestionRatings": List(
                                    Struct({"name": String(), "rating": String()})
                                ),
                            }
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
                        "relatedDocuments": List(
                            Struct({"documentType": String(), "documentUri": String()})
                        ),
                    }
                )
            ),
        ),
        ("unpublishedReports", List(Struct({"firstVisitDate": String()}))),
        (
            "providerInspectionAreas",
            List(
                Struct(
                    {
                        "inspectionAreaId": String(),
                        "reports": List(
                            Struct(
                                {
                                    "inspectionId": String(),
                                    "reportLinkId": String(),
                                    "providerId": String(),
                                    "locationId": String(),
                                }
                            )
                        ),
                    }
                )
            ),
        ),
        ("specialism", List(Struct({"code": String(), "name": String()}))),
        ("ageGroup", List(Struct({"code": String(), "name": String()}))),
        (
            "settingServices",
            List(
                Struct(
                    {
                        "settingtypeCode": String(),
                        "settingtypeName": String(),
                        "servicetypeCode": String(),
                        "servicetypeName": String(),
                    }
                )
            ),
        ),
    ]
)
