import polars as pl

raw_locations_schema = pl.Schema(
    [
        ("locationId", pl.String()),
        ("providerId", pl.String()),
        ("organisationType", pl.String()),
        ("type", pl.String()),
        ("name", pl.String()),
        ("brandId", pl.String()),
        ("brandName", pl.String()),
        ("onspdCcgCode", pl.String()),
        ("onspdCcgName", pl.String()),
        ("odsCcgCode", pl.String()),
        ("odsCcgName", pl.String()),
        ("onspdIcbCode", pl.String()),
        ("onspdIcbName", pl.String()),
        ("odsCode", pl.String()),
        ("registrationStatus", pl.String()),
        ("registrationDate", pl.String()),
        ("deregistrationDate", pl.String()),
        ("dormancy", pl.String()),
        ("dormancyStartDate", pl.String()),
        ("dormancyEndDate", pl.String()),
        ("alsoKnownAs", pl.String()),
        ("onspdLatitude", pl.String()),
        ("onspdLongitude", pl.String()),
        ("careHome", pl.String()),
        ("inspectionDirectorate", pl.String()),
        ("website", pl.String()),
        ("postalAddressLine1", pl.String()),
        ("postalAddressLine2", pl.String()),
        ("postalAddressTownCity", pl.String()),
        ("postalAddressCounty", pl.String()),
        ("region", pl.String()),
        ("postalCode", pl.String()),
        ("uprn", pl.String()),
        ("mainPhoneNumber", pl.String()),
        ("registeredManagerAbsentDate", pl.String()),
        ("numberOfBeds", pl.Int32()),
        ("constituency", pl.String()),
        ("localAuthority", pl.String()),
        ("lastInspection", pl.Struct({"date": pl.String()})),
        ("lastReport", pl.Struct({"publicationDate": pl.String()})),
        (
            "relationships",
            pl.List(
                pl.Struct(
                    {
                        "relatedLocationId": pl.String(),
                        "relatedLocationName": pl.String(),
                        "type": pl.String(),
                        "reason": pl.String(),
                    }
                )
            ),
        ),
        ("locationTypes", pl.List(pl.Struct({"type": pl.String()}))),
        (
            "regulatedActivities",
            pl.List(
                pl.Struct(
                    {
                        "name": pl.String(),
                        "code": pl.String(),
                        "contacts": pl.List(
                            pl.Struct(
                                {
                                    "personFamilyName": pl.String(),
                                    "personGivenName": pl.String(),
                                    "personRoles": pl.List(pl.String()),
                                    "personTitle": pl.String(),
                                    "col3": pl.List(pl.String()),
                                }
                            )
                        ),
                    }
                )
            ),
        ),
        (
            "gacServiceTypes",
            pl.List(pl.Struct({"name": pl.String(), "description": pl.String()})),
        ),
        ("specialisms", pl.List(pl.Struct({"name": pl.String()}))),
        (
            "inspectionCategories",
            pl.List(
                pl.Struct(
                    {"primary": pl.String(), "code": pl.String(), "name": pl.String()}
                )
            ),
        ),
        (
            "inspectionAreas",
            pl.List(
                pl.Struct(
                    {
                        "inspectionAreaId": pl.String(),
                        "inspectionAreaName": pl.String(),
                        "inspectionAreaType": pl.String(),
                        "status": pl.String(),
                        "supersededBy": pl.List(pl.String()),
                        "endDate": pl.String(),
                    }
                )
            ),
        ),
        (
            "currentRatings",
            pl.Struct(
                {
                    "overall": pl.Struct(
                        {
                            "organisationId": pl.String(),
                            "rating": pl.String(),
                            "reportDate": pl.String(),
                            "reportLinkId": pl.String(),
                            "useOfResources": pl.Struct(
                                {
                                    "organisationId": pl.String(),
                                    "summary": pl.String(),
                                    "useOfResourcesRating": pl.String(),
                                    "combinedQualitySummary": pl.String(),
                                    "combinedQualityRating": pl.String(),
                                    "reportDate": pl.String(),
                                    "reportLinkId": pl.String(),
                                }
                            ),
                            "keyQuestionRatings": pl.List(
                                pl.Struct(
                                    {
                                        "name": pl.String(),
                                        "rating": pl.String(),
                                        "reportDate": pl.String(),
                                        "organisationId": pl.String(),
                                        "reportLinkId": pl.String(),
                                    }
                                )
                            ),
                        }
                    ),
                    "serviceRatings": pl.List(
                        pl.Struct(
                            {
                                "name": pl.String(),
                                "rating": pl.String(),
                                "reportDate": pl.String(),
                                "organisationId": pl.String(),
                                "reportLinkId": pl.String(),
                                "keyQuestionRatings": pl.List(
                                    pl.Struct(
                                        {"name": pl.String(), "rating": pl.String()}
                                    )
                                ),
                            }
                        )
                    ),
                }
            ),
        ),
        (
            "historicRatings",
            pl.List(
                pl.Struct(
                    {
                        "reportDate": pl.String(),
                        "reportLinkId": pl.String(),
                        "organisationId": pl.String(),
                        "serviceRatings": pl.List(
                            pl.Struct(
                                {
                                    "name": pl.String(),
                                    "rating": pl.String(),
                                    "keyQuestionRatings": pl.List(
                                        pl.Struct(
                                            {"name": pl.String(), "rating": pl.String()}
                                        )
                                    ),
                                }
                            )
                        ),
                        "overall": pl.Struct(
                            {
                                "rating": pl.String(),
                                "useOfResources": pl.Struct(
                                    {
                                        "combinedQualityRating": pl.String(),
                                        "combinedQualitySummary": pl.String(),
                                        "useOfResourcesRating": pl.String(),
                                        "useOfResourcesSummary": pl.String(),
                                    }
                                ),
                                "keyQuestionRatings": pl.List(
                                    pl.Struct(
                                        {"name": pl.String(), "rating": pl.String()}
                                    )
                                ),
                            }
                        ),
                    }
                )
            ),
        ),
        (
            "reports",
            pl.List(
                pl.Struct(
                    {
                        "linkId": pl.String(),
                        "reportDate": pl.String(),
                        "firstVisitDate": pl.String(),
                        "reportUri": pl.String(),
                        "reportType": pl.String(),
                        "relatedDocuments": pl.List(
                            pl.Struct(
                                {
                                    "documentType": pl.String(),
                                    "documentUri": pl.String(),
                                }
                            )
                        ),
                    }
                )
            ),
        ),
        ("unpublishedReports", pl.List(pl.Struct({"firstVisitDate": pl.String()}))),
        (
            "providerInspectionAreas",
            pl.List(
                pl.Struct(
                    {
                        "inspectionAreaId": pl.String(),
                        "reports": pl.List(
                            pl.Struct(
                                {
                                    "inspectionId": pl.String(),
                                    "reportLinkId": pl.String(),
                                    "providerId": pl.String(),
                                    "locationId": pl.String(),
                                }
                            )
                        ),
                    }
                )
            ),
        ),
        ("specialism", pl.List(pl.Struct({"code": pl.String(), "name": pl.String()}))),
        ("ageGroup", pl.List(pl.Struct({"code": pl.String(), "name": pl.String()}))),
        (
            "settingServices",
            pl.List(
                pl.Struct(
                    {
                        "settingtypeCode": pl.String(),
                        "settingtypeName": pl.String(),
                        "servicetypeCode": pl.String(),
                        "servicetypeName": pl.String(),
                    }
                )
            ),
        ),
    ]
)
