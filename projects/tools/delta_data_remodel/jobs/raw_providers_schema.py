import polars as pl

raw_providers_schema = pl.Schema(
    [
        ("providerId", pl.String()),
        ("locationIds", pl.List(pl.String())),
        ("organisationType", pl.String()),
        ("ownershipType", pl.String()),
        ("type", pl.String()),
        ("name", pl.String()),
        ("brandId", pl.String()),
        ("brandName", pl.String()),
        ("odsCode", pl.String()),
        ("registrationStatus", pl.String()),
        ("registrationDate", pl.String()),
        ("companiesHouseNumber", pl.String()),
        ("charityNumber", pl.String()),
        ("website", pl.String()),
        ("postalAddressLine1", pl.String()),
        ("postalAddressLine2", pl.String()),
        ("postalAddressTownCity", pl.String()),
        ("postalAddressCounty", pl.String()),
        ("region", pl.String()),
        ("postalCode", pl.String()),
        ("alsoKnownAs", pl.String()),
        ("deregistrationDate", pl.String()),
        ("uprn", pl.String()),
        ("onspdLatitude", pl.String()),
        ("onspdLongitude", pl.String()),
        ("onspdIcbCode", pl.String()),
        ("onspdIcbName", pl.String()),
        ("mainPhoneNumber", pl.String()),
        ("inspectionDirectorate", pl.String()),
        ("constituency", pl.String()),
        ("localAuthority", pl.String()),
        ("lastInspection", pl.Struct({"date": pl.String()})),
        ("lastReport", pl.Struct({"publicationDate": pl.String()})),
        (
            "contacts",
            pl.List(
                pl.Struct(
                    {
                        "personTitle": pl.String(),
                        "personGivenName": pl.String(),
                        "personFamilyName": pl.String(),
                        "personRoles": pl.List(pl.String()),
                    }
                )
            ),
        ),
        (
            "relationships",
            pl.List(
                pl.Struct(
                    {
                        "relatedProviderId": pl.String(),
                        "relatedProviderName": pl.String(),
                        "type": pl.String(),
                        "reason": pl.String(),
                    }
                )
            ),
        ),
        (
            "regulatedActivities",
            pl.List(
                pl.Struct(
                    {
                        "name": pl.String(),
                        "code": pl.String(),
                        "nominatedIndividual": pl.Struct(
                            {
                                "personTitle": pl.String(),
                                "personGivenName": pl.String(),
                                "personFamilyName": pl.String(),
                            }
                        ),
                    }
                )
            ),
        ),
        (
            "inspectionCategories",
            pl.List(
                pl.Struct(
                    {"code": pl.String(), "primary": pl.String(), "name": pl.String()}
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
                        "endDate": pl.String(),
                        "supersededBy": pl.List(pl.String()),
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
                            "rating": pl.String(),
                            "reportDate": pl.String(),
                            "reportLinkId": pl.String(),
                            "useOfResources": pl.Struct(
                                {
                                    "useOfResourcesSummary": pl.String(),
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
                        "overall": pl.Struct(
                            {
                                "rating": pl.String(),
                                "useOfResources": pl.Struct(
                                    {
                                        "useOfResourcesSummary": pl.String(),
                                        "useOfResourcesRating": pl.String(),
                                        "combinedQualitySummary": pl.String(),
                                        "combinedQualityRating": pl.String(),
                                    }
                                ),
                                "keyQuestionRatings": pl.List(
                                    pl.Struct(
                                        {"name": pl.String(), "rating": pl.String()}
                                    )
                                ),
                            }
                        ),
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
                        "inspectionLocations": pl.List(
                            pl.Struct({"locationId": pl.String()})
                        ),
                        "relatedDocuments": pl.List(
                            pl.Struct(
                                {
                                    "documentUri": pl.String(),
                                    "documentType": pl.String(),
                                }
                            )
                        ),
                    }
                )
            ),
        ),
        ("unpublishedReports", pl.List(pl.Struct({"firstVisitDate": pl.String()}))),
    ]
)
