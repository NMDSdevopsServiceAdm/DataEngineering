from pyspark.sql.types import (
    StructField,
    StructType,
    StringType,
    ArrayType,
    FloatType,
    IntegerType,
    DoubleType,
    LongType,
)

from utils.column_names.raw_data_files.cqc_location_api_columns import (
    OldCqcLocationApiColumns as OldColNames,
)
from utils.column_names.raw_data_files.cqc_location_api_columns import (
    NewCqcLocationApiColumns as NewColNames,
)

LOCATION_SCHEMA_NEW = StructType(
    [
        StructField(NewColNames.also_known_as, StringType(), True),
        StructField(NewColNames.brand_id, StringType(), True),
        StructField(NewColNames.brand_name, StringType(), True),
        StructField(NewColNames.care_home, StringType(), True),
        StructField(NewColNames.constituency, StringType(), True),
        StructField(
            NewColNames.current_ratings,
            StructType(
                [
                    StructField(
                        NewColNames.overall,
                        StructType(
                            [
                                StructField(
                                    NewColNames.key_question_ratings,
                                    ArrayType(
                                        StructType(
                                            [
                                                StructField(
                                                    NewColNames.name, StringType(), True
                                                ),
                                                StructField(
                                                    NewColNames.organisation_id,
                                                    StringType(),
                                                    True,
                                                ),
                                                StructField(
                                                    NewColNames.rating,
                                                    StringType(),
                                                    True,
                                                ),
                                                StructField(
                                                    NewColNames.report_date,
                                                    StringType(),
                                                    True,
                                                ),
                                                StructField(
                                                    NewColNames.report_link_id,
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
                                    NewColNames.organisation_id, StringType(), True
                                ),
                                StructField(NewColNames.rating, StringType(), True),
                                StructField(
                                    NewColNames.report_date, StringType(), True
                                ),
                                StructField(
                                    NewColNames.report_link_id, StringType(), True
                                ),
                                StructField(
                                    NewColNames.use_of_resources,
                                    StructType(
                                        [
                                            StructField(
                                                NewColNames.combined_quality_rating,
                                                StringType(),
                                                True,
                                            ),
                                            StructField(
                                                NewColNames.combined_quality_summary,
                                                StringType(),
                                                True,
                                            ),
                                            StructField(
                                                NewColNames.organisation_id,
                                                StringType(),
                                                True,
                                            ),
                                            StructField(
                                                NewColNames.report_date,
                                                StringType(),
                                                True,
                                            ),
                                            StructField(
                                                NewColNames.report_link_id,
                                                StringType(),
                                                True,
                                            ),
                                            StructField(
                                                NewColNames.summary, StringType(), True
                                            ),
                                            StructField(
                                                NewColNames.useOfResourcesRating,
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
                                        NewColNames.key_question_ratings,
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
                                        NewColNames.organisation_id, StringType(), True
                                    ),
                                    StructField(NewColNames.rating, StringType(), True),
                                    StructField(
                                        NewColNames.report_date, StringType(), True
                                    ),
                                    StructField(
                                        NewColNames.report_link_id, StringType(), True
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
        StructField(NewColNames.dormancy, StringType(), True),
        StructField(NewColNames.dormancyEndDate, StringType(), True),
        StructField(NewColNames.dormancyStartDate, StringType(), True),
        StructField(
            NewColNames.gacServiceTypes,
            ArrayType(
                StructType(
                    [
                        StructField(NewColNames.description, StringType(), True),
                        StructField(NewColNames.name, StringType(), True),
                    ]
                ),
                True,
            ),
            True,
        ),
        StructField(
            NewColNames.historicRatings,
            ArrayType(
                StructType(
                    [
                        StructField(NewColNames.organisation_id, StringType(), True),
                        StructField(
                            NewColNames.overall,
                            StructType(
                                [
                                    StructField(
                                        NewColNames.key_question_ratings,
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
                                        NewColNames.use_of_resources,
                                        StructType(
                                            [
                                                StructField(
                                                    NewColNames.combined_quality_rating,
                                                    StringType(),
                                                    True,
                                                ),
                                                StructField(
                                                    NewColNames.combined_quality_summary,
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
                        StructField(NewColNames.report_date, StringType(), True),
                        StructField(NewColNames.report_link_id, StringType(), True),
                        StructField(
                            NewColNames.serviceRatings,
                            ArrayType(
                                StructType(
                                    [
                                        StructField(
                                            NewColNames.key_question_ratings,
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
        StructField(NewColNames.locationId, StringType(), True),
        StructField(
            NewColNames.locationTypes,
            ArrayType(
                StructType([StructField(NewColNames.type, StringType(), True)]), True
            ),
            True,
        ),
        StructField(NewColNames.mainPhoneNumber, StringType(), True),
        StructField(NewColNames.name, StringType(), True),
        StructField(NewColNames.numberOfBeds, LongType(), True),
        StructField(NewColNames.odsCcgCode, StringType(), True),
        StructField(NewColNames.odsCcgName, StringType(), True),
        StructField(NewColNames.odsCode, StringType(), True),
        StructField(NewColNames.onspdCcgCode, StringType(), True),
        StructField(NewColNames.onspdCcgName, StringType(), True),
        StructField(NewColNames.onspdIcbCode, StringType(), True),
        StructField(NewColNames.onspdIcbName, StringType(), True),
        StructField(NewColNames.onspdLatitude, DoubleType(), True),
        StructField(NewColNames.onspdLongitude, DoubleType(), True),
        StructField(NewColNames.organisationType, StringType(), True),
        StructField(NewColNames.postalAddressCounty, StringType(), True),
        StructField(NewColNames.postalAddressLine1, StringType(), True),
        StructField(NewColNames.postalAddressLine2, StringType(), True),
        StructField(NewColNames.postalAddressTownCity, StringType(), True),
        StructField(NewColNames.postalCode, StringType(), True),
        StructField(NewColNames.providerId, StringType(), True),
        StructField(
            NewColNames.providerInspectionAreas,
            ArrayType(
                StructType(
                    [
                        StructField(NewColNames.inspectionAreaId, StringType(), True),
                        StructField(
                            NewColNames.reports,
                            ArrayType(
                                StructType(
                                    [
                                        StructField(
                                            NewColNames.inspectionId, StringType(), True
                                        ),
                                        StructField(
                                            NewColNames.locationId, StringType(), True
                                        ),
                                        StructField(
                                            NewColNames.providerId, StringType(), True
                                        ),
                                        StructField(
                                            NewColNames.report_link_id,
                                            StringType(),
                                            True,
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
        StructField(NewColNames.region, StringType(), True),
        StructField(NewColNames.registeredManagerAbsentDate, StringType(), True),
        StructField(NewColNames.registrationDate, StringType(), True),
        StructField(NewColNames.registrationStatus, StringType(), True),
        StructField(
            NewColNames.regulatedActivities,
            ArrayType(
                StructType(
                    [
                        StructField(NewColNames.code, StringType(), True),
                        StructField(
                            NewColNames.contacts,
                            ArrayType(
                                StructType(
                                    [
                                        StructField(
                                            NewColNames.personFamilyName,
                                            StringType(),
                                            True,
                                        ),
                                        StructField(
                                            NewColNames.personGivenName,
                                            StringType(),
                                            True,
                                        ),
                                        StructField(
                                            NewColNames.personRoles,
                                            ArrayType(StringType(), True),
                                            True,
                                        ),
                                        StructField(
                                            NewColNames.personTitle, StringType(), True
                                        ),
                                    ]
                                ),
                                True,
                            ),
                            True,
                        ),
                        StructField(NewColNames.name, StringType(), True),
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
                        StructField(NewColNames.relatedLocationId, StringType(), True),
                        StructField(
                            NewColNames.relatedLocationName, StringType(), True
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
                        StructField(NewColNames.report_date, StringType(), True),
                        StructField(NewColNames.reportType, StringType(), True),
                        StructField(NewColNames.reportUri, StringType(), True),
                    ]
                ),
                True,
            ),
            True,
        ),
        StructField(
            NewColNames.specialisms,
            ArrayType(
                StructType([StructField(NewColNames.name, StringType(), True)]), True
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

LOCATION_SCHEMA = StructType(
    fields=[
        StructField(NewColNames.location_id, StringType(), True),
        StructField(NewColNames.provider_id, StringType(), True),
        StructField(NewColNames.organisation_type, StringType(), True),
        StructField(NewColNames.type, StringType(), True),
        StructField(NewColNames.name, StringType(), True),
        StructField(NewColNames.ccg_code, StringType(), True),
        StructField(NewColNames.ccg_name, StringType(), True),
        StructField(NewColNames.ods_code, StringType(), True),
        StructField(NewColNames.uprn, StringType(), True),
        StructField(NewColNames.registration_status, StringType(), True),
        StructField(NewColNames.registration_date, StringType(), True),
        StructField(NewColNames.deregistration_date, StringType(), True),
        StructField(NewColNames.dormancy, StringType(), True),
        StructField(NewColNames.number_of_beds, IntegerType(), True),
        StructField(NewColNames.website, StringType(), True),
        StructField(NewColNames.address_line_one, StringType(), True),
        StructField(NewColNames.town_or_city, StringType(), True),
        StructField(NewColNames.county, StringType(), True),
        StructField(NewColNames.region, StringType(), True),
        StructField(NewColNames.postcode, StringType(), True),
        StructField(NewColNames.latitude, StringType(), True),
        StructField(NewColNames.longitude, StringType(), True),
        StructField(NewColNames.care_home, StringType(), True),
        StructField(NewColNames.inspection_directorate, StringType(), True),
        StructField(NewColNames.phone_number, StringType(), True),
        StructField(NewColNames.constituancy, StringType(), True),
        StructField(NewColNames.local_authority, StringType(), True),
        StructField(
            NewColNames.last_inspection,
            StructType([StructField(NewColNames.date, StringType(), True)]),
            True,
        ),
        StructField(
            NewColNames.last_report,
            StructType([StructField(NewColNames.publication_date, StringType(), True)]),
            True,
        ),
        StructField(
            NewColNames.relationships,
            ArrayType(
                StructType(
                    [
                        StructField(
                            NewColNames.related_location_id, StringType(), True
                        ),
                        StructField(
                            NewColNames.related_location_name, StringType(), True
                        ),
                        StructField(NewColNames.type, StringType(), True),
                        StructField(NewColNames.reason, StringType(), True),
                    ]
                )
            ),
            True,
        ),
        StructField(
            NewColNames.regulated_activities,
            ArrayType(
                StructType(
                    [
                        StructField(NewColNames.name, StringType(), True),
                        StructField(NewColNames.code, StringType(), True),
                        StructField(
                            NewColNames.contacts,
                            ArrayType(
                                StructType(
                                    [
                                        StructField(
                                            NewColNames.title, StringType(), True
                                        ),
                                        StructField(
                                            NewColNames.given_name, StringType(), True
                                        ),
                                        StructField(
                                            NewColNames.family_name, StringType(), True
                                        ),
                                        StructField(
                                            NewColNames.roles, StringType(), True
                                        ),
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
            NewColNames.gac_service_types,
            ArrayType(
                StructType(
                    [
                        StructField(NewColNames.name, StringType(), True),
                        StructField(NewColNames.description, StringType(), True),
                    ]
                )
            ),
        ),
        StructField(
            NewColNames.inspection_categories,
            ArrayType(
                StructType(
                    [
                        StructField(NewColNames.code, StringType(), True),
                        StructField(NewColNames.primary, StringType(), True),
                        StructField(NewColNames.name, StringType(), True),
                    ]
                )
            ),
            True,
        ),
        StructField(
            NewColNames.specialisms,
            ArrayType(
                StructType(
                    [
                        StructField(NewColNames.name, StringType(), True),
                    ]
                )
            ),
            True,
        ),
        StructField(
            NewColNames.current_ratings,
            StructType(
                [
                    StructField(
                        NewColNames.overall,
                        StructType(
                            [
                                StructField(NewColNames.rating, StringType(), True),
                                StructField(
                                    NewColNames.report_date, StringType(), True
                                ),
                                StructField(
                                    NewColNames.report_link_id, StringType(), True
                                ),
                                StructField(
                                    NewColNames.key_question_ratings,
                                    ArrayType(
                                        StructType(
                                            [
                                                StructField(
                                                    NewColNames.name, StringType(), True
                                                ),
                                                StructField(
                                                    NewColNames.rating,
                                                    StringType(),
                                                    True,
                                                ),
                                                StructField(
                                                    NewColNames.report_date,
                                                    StringType(),
                                                    True,
                                                ),
                                                StructField(
                                                    NewColNames.report_link_id,
                                                    StringType(),
                                                    True,
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
            NewColNames.historic_ratings,
            ArrayType(
                StructType(
                    [
                        StructField(NewColNames.organisation_id, StringType(), True),
                        StructField(NewColNames.report_link_id, StringType(), True),
                        StructField(NewColNames.report_date, StringType(), True),
                        StructField(
                            NewColNames.overall,
                            StructType(
                                [
                                    StructField(NewColNames.rating, StringType(), True),
                                    StructField(
                                        NewColNames.key_question_ratings,
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
            NewColNames.reports,
            ArrayType(
                StructType(
                    [
                        StructField(NewColNames.link_id, StringType(), True),
                        StructField(NewColNames.report_date, StringType(), True),
                        StructField(NewColNames.report_uri, StringType(), True),
                        StructField(NewColNames.first_visit_date, StringType(), True),
                        StructField(NewColNames.report_type, StringType(), True),
                    ]
                )
            ),
            True,
        ),
    ]
)
