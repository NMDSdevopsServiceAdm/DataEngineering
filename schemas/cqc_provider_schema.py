from pyspark.sql.types import (
    StructField,
    StructType,
    StringType,
    ArrayType,
    FloatType,
)

from utils.column_names.raw_data_files.cqc_provider_api_columns import (
    CqcProviderApiColumns as ColNames,
)

PROVIDER_SCHEMA = StructType(
    [
        StructField(ColNames.provider_id, StringType(), True),
        StructField(ColNames.location_ids, ArrayType(StringType(), True), True),
        StructField(ColNames.organisation_type, StringType(), True),
        StructField(ColNames.ownership_type, StringType(), True),
        StructField(ColNames.type, StringType(), True),
        StructField(ColNames.name, StringType(), True),
        StructField(ColNames.brand_id, StringType(), True),
        StructField(ColNames.brand_name, StringType(), True),
        StructField(ColNames.ods_code, StringType(), True),
        StructField(ColNames.registration_status, StringType(), True),
        StructField(ColNames.registration_date, StringType(), True),
        StructField(ColNames.companies_house_number, StringType(), True),
        StructField(ColNames.charity_number, StringType(), True),
        StructField(ColNames.website, StringType(), True),
        StructField(ColNames.postal_address_line1, StringType(), True),
        StructField(ColNames.postal_address_line2, StringType(), True),
        StructField(ColNames.postal_address_town_city, StringType(), True),
        StructField(ColNames.postal_address_county, StringType(), True),
        StructField(ColNames.region, StringType(), True),
        StructField(ColNames.postal_code, StringType(), True),
        StructField(ColNames.also_known_as, StringType(), True),
        StructField(ColNames.deregistration_date, StringType(), True),
        StructField(ColNames.uprn, StringType(), True),
        StructField(ColNames.onspd_latitude, StringType(), True),
        StructField(ColNames.onspd_longitude, StringType(), True),
        StructField(ColNames.onspd_icb_code, StringType(), True),
        StructField(ColNames.onspd_icb_name, StringType(), True),
        StructField(ColNames.main_phone_number, StringType(), True),
        StructField(ColNames.inspection_directorate, StringType(), True),
        StructField(ColNames.constituency, StringType(), True),
        StructField(ColNames.local_authority, StringType(), True),
        StructField(
            ColNames.last_inspection,
            StructType([StructField(ColNames.date, StringType(), True)]),
            True,
        ),
        StructField(
            ColNames.last_report,
            StructType([StructField(ColNames.publication_date, StringType(), True)]),
            True,
        ),
        StructField(
            ColNames.contacts,
            ArrayType(
                StructType(
                    [
                        StructField(ColNames.person_title, StringType(), True),
                        StructField(ColNames.person_given_name, StringType(), True),
                        StructField(ColNames.person_family_name, StringType(), True),
                        StructField(
                            ColNames.person_roles,
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
            ColNames.relationships,
            ArrayType(
                StructType(
                    [
                        StructField(
                            ColNames.related_provider_id, StringType(), True
                        ),
                        StructField(
                            ColNames.related_provider_name, StringType(), True
                        ),
                        StructField(ColNames.type, StringType(), True),
                        StructField(ColNames.reason, StringType(), True),
                    ]
                ),
                True,
            ),
            True,
        ),
        StructField(
            ColNames.regulated_activities,
            ArrayType(
                StructType(
                    [
                        StructField(ColNames.name, StringType(), True),
                        StructField(ColNames.code, StringType(), True),
                        StructField(
                            ColNames.nominated_individual,
                            StructType(
                                [
                                    StructField(
                                        ColNames.person_title, StringType(), True
                                    ),
                                    StructField(
                                        ColNames.person_given_name,
                                        StringType(),
                                        True,
                                    ),
                                    StructField(
                                        ColNames.person_family_name,
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
            True,
        ),
        StructField(
            ColNames.inspection_categories,
            ArrayType(
                StructType(
                    [
                        StructField(ColNames.code, StringType(), True),
                        StructField(ColNames.primary, StringType(), True),
                        StructField(ColNames.name, StringType(), True),
                    ]
                ),
                True,
            ),
            True,
        ),
        StructField(
            ColNames.inspection_areas,
            ArrayType(
                StructType(
                    [
                        StructField(ColNames.inspection_area_id, StringType(), True),
                        StructField(
                            ColNames.inspection_area_name, StringType(), True
                        ),
                        StructField(
                            ColNames.inspection_area_type, StringType(), True
                        ),
                        StructField(ColNames.status, StringType(), True),
                        StructField(ColNames.end_date, StringType(), True),
                        StructField(
                            ColNames.superseded_by,
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
            ColNames.current_ratings,
            StructType(
                [
                    StructField(
                        ColNames.overall,
                        StructType(
                            [
                                StructField(ColNames.rating, StringType(), True),
                                StructField(
                                    ColNames.report_date, StringType(), True
                                ),
                                StructField(
                                    ColNames.report_link_id, StringType(), True
                                ),
                                StructField(
                                    ColNames.use_of_resources,
                                    StructType(
                                        [
                                            StructField(
                                                ColNames.use_of_resources_summary,
                                                StringType(),
                                                True,
                                            ),
                                            StructField(
                                                ColNames.use_of_resources_rating,
                                                StringType(),
                                                True,
                                            ),
                                            StructField(
                                                ColNames.combined_quality_summary,
                                                StringType(),
                                                True,
                                            ),
                                            StructField(
                                                ColNames.combined_quality_rating,
                                                StringType(),
                                                True,
                                            ),
                                            StructField(
                                                ColNames.report_date,
                                                StringType(),
                                                True,
                                            ),
                                            StructField(
                                                ColNames.report_link_id,
                                                StringType(),
                                                True,
                                            ),
                                        ]
                                    ),
                                    True,
                                ),
                                StructField(
                                    ColNames.key_question_ratings,
                                    ArrayType(
                                        StructType(
                                            [
                                                StructField(
                                                    ColNames.name, StringType(), True
                                                ),
                                                StructField(
                                                    ColNames.rating,
                                                    StringType(),
                                                    True,
                                                ),
                                                StructField(
                                                    ColNames.report_date,
                                                    StringType(),
                                                    True,
                                                ),
                                                StructField(
                                                    ColNames.organisation_id,
                                                    StringType(),
                                                    True,
                                                ),
                                                StructField(
                                                    ColNames.report_link_id,
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
                    StructField(
                        ColNames.service_ratings,
                        ArrayType(
                            StructType(
                                [
                                    StructField(ColNames.name, StringType(), True),
                                    StructField(ColNames.rating, StringType(), True),
                                    StructField(
                                        ColNames.report_date, StringType(), True
                                    ),
                                    StructField(
                                        ColNames.organisation_id, StringType(), True
                                    ),
                                    StructField(
                                        ColNames.report_link_id, StringType(), True
                                    ),
                                    StructField(
                                        ColNames.key_question_ratings,
                                        ArrayType(
                                            StructType(
                                                [
                                                    StructField(
                                                        ColNames.name,
                                                        StringType(),
                                                        True,
                                                    ),
                                                    StructField(
                                                        ColNames.rating,
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
                ]
            ),
            True,
        ),
        StructField(
            ColNames.historic_ratings,
            ArrayType(
                StructType(
                    [
                        StructField(ColNames.report_date, StringType(), True),
                        StructField(ColNames.report_link_id, StringType(), True),
                        StructField(ColNames.organisation_id, StringType(), True),
                        StructField(
                            ColNames.overall,
                            StructType(
                                [
                                    StructField(ColNames.rating, StringType(), True),
                                    StructField(
                                        ColNames.use_of_resources,
                                        StructType(
                                            [
                                                StructField(
                                                    ColNames.use_of_resources_summary,
                                                    StringType(),
                                                    True,
                                                ),
                                                StructField(
                                                    ColNames.use_of_resources_rating,
                                                    StringType(),
                                                    True,
                                                ),
                                                StructField(
                                                    ColNames.combined_quality_summary,
                                                    StringType(),
                                                    True,
                                                ),
                                                StructField(
                                                    ColNames.combined_quality_rating,
                                                    StringType(),
                                                    True,
                                                ),
                                            ]
                                        ),
                                        True,
                                    ),
                                    StructField(
                                        ColNames.key_question_ratings,
                                        ArrayType(
                                            StructType(
                                                [
                                                    StructField(
                                                        ColNames.name,
                                                        StringType(),
                                                        True,
                                                    ),
                                                    StructField(
                                                        ColNames.rating,
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
                        StructField(
                            ColNames.service_ratings,
                            ArrayType(
                                StructType(
                                    [
                                        StructField(
                                            ColNames.name, StringType(), True
                                        ),
                                        StructField(
                                            ColNames.rating, StringType(), True
                                        ),
                                        StructField(
                                            ColNames.key_question_ratings,
                                            ArrayType(
                                                StructType(
                                                    [
                                                        StructField(
                                                            ColNames.name,
                                                            StringType(),
                                                            True,
                                                        ),
                                                        StructField(
                                                            ColNames.rating,
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
                    ]
                ),
                True,
            ),
            True,
        ),
        StructField(
            ColNames.reports,
            ArrayType(
                StructType(
                    [
                        StructField(ColNames.link_id, StringType(), True),
                        StructField(ColNames.report_date, StringType(), True),
                        StructField(ColNames.first_visit_date, StringType(), True),
                        StructField(ColNames.report_uri, StringType(), True),
                        StructField(ColNames.report_type, StringType(), True),
                        StructField(
                            ColNames.inspection_locations,
                            ArrayType(
                                StructType(
                                    [
                                        StructField(
                                            ColNames.location_id, StringType(), True
                                        )
                                    ]
                                ),
                                True,
                            ),
                            True,
                        ),
                        StructField(
                            ColNames.related_documents,
                            ArrayType(
                                StructType(
                                    [
                                        StructField(
                                            ColNames.document_uri, StringType(), True
                                        ),
                                        StructField(
                                            ColNames.document_type,
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
        StructField(
            ColNames.unpublished_reports,
            ArrayType(
                StructType(
                    [StructField(ColNames.first_visit_date, StringType(), True)]
                ),
                True,
            ),
            True,
        ),
    ]
)
