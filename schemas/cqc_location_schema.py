from pyspark.sql.types import (
    StructField,
    StructType,
    StringType,
    ArrayType,
    FloatType,
    IntegerType,
)

LOCATION_SCHEMA = StructType(
    fields=[
        StructField("locationId", StringType(), True),
        StructField("providerId", StringType(), True),
        StructField("organisationType", StringType(), True),
        StructField("type", StringType(), True),
        StructField("name", StringType(), True),
        StructField("onspdCcgCode", StringType(), True),
        StructField("onspdCcgName", StringType(), True),
        StructField("odsCode", StringType(), True),
        StructField("uprn", StringType(), True),
        StructField("registrationStatus", StringType(), True),
        StructField("registrationDate", StringType(), True),
        StructField("deregistrationDate", StringType(), True),
        StructField("dormancy", StringType(), True),
        StructField("numberOfBeds", IntegerType(), True),
        StructField("website", StringType(), True),
        StructField("postalAddressLine1", StringType(), True),
        StructField("postalAddressTownCity", StringType(), True),
        StructField("postalAddressCounty", StringType(), True),
        StructField("region", StringType(), True),
        StructField("postalCode", StringType(), True),
        StructField("onspdLatitude", FloatType(), True),
        StructField("onspdLongitude", FloatType(), True),
        StructField("careHome", StringType(), True),
        StructField("inspectionDirectorate", StringType(), True),
        StructField("mainPhoneNumber", StringType(), True),
        StructField("constituency", StringType(), True),
        StructField("localAuthority", StringType(), True),
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
        StructField(
            "relationships",
            ArrayType(
                StructType(
                    [
                        StructField("relatedLocationId", StringType(), True),
                        StructField("relatedLocationName", StringType(), True),
                        StructField("type", StringType(), True),
                        StructField("reason", StringType(), True),
                    ]
                )
            ),
            True,
        ),
        StructField(
            "regulatedActivities",
            ArrayType(
                StructType(
                    [
                        StructField("name", StringType(), True),
                        StructField("code", StringType(), True),
                        StructField(
                            "contacts",
                            ArrayType(
                                StructType(
                                    [
                                        StructField("personTitle", StringType(), True),
                                        StructField(
                                            "personGivenName", StringType(), True
                                        ),
                                        StructField(
                                            "personFamilyName", StringType(), True
                                        ),
                                        StructField("personRoles", StringType(), True),
                                    ]
                                )
                            ),
                            True,
                        ),
                    ]
                )
            ),
        ),
        StructField(
            "gacServiceTypes",
            ArrayType(
                StructType(
                    [
                        StructField("name", StringType(), True),
                        StructField("description", StringType(), True),
                    ]
                )
            ),
        ),
        StructField(
            "inspectionCategories",
            ArrayType(
                StructType(
                    [
                        StructField("code", StringType(), True),
                        StructField("primary", StringType(), True),
                        StructField("name", StringType(), True),
                    ]
                )
            ),
            True,
        ),
        StructField(
            "specialisms",
            ArrayType(
                StructType(
                    [
                        StructField("name", StringType(), True),
                    ]
                )
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
                                StructField("rating", StringType(), True),
                                StructField("reportDate", StringType(), True),
                                StructField("reportLinkId", StringType(), True),
                                StructField(
                                    "keyQuestionRatings",
                                    ArrayType(
                                        StructType(
                                            [
                                                StructField("name", StringType(), True),
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
                                        )
                                    ),
                                ),
                            ]
                        ),
                        True,
                    )
                ]
            ),
            True,
        ),
        StructField(
            "historicRatings",
            ArrayType(
                StructType(
                    [
                        StructField("organisationId", StringType(), True),
                        StructField("reportLinkId", StringType(), True),
                        StructField("reportDate", StringType(), True),
                        StructField(
                            "overall",
                            StructType(
                                [
                                    StructField("rating", StringType(), True),
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
                                ]
                            ),
                            True,
                        ),
                    ]
                ),
                True,
            ),
        ),
        StructField(
            "reports",
            ArrayType(
                StructType(
                    [
                        StructField("linkId", StringType(), True),
                        StructField("reportDate", StringType(), True),
                        StructField("reportUri", StringType(), True),
                        StructField("firstVisitDate", StringType(), True),
                        StructField("reportType", StringType(), True),
                    ]
                )
            ),
            True,
        ),
    ]
)
