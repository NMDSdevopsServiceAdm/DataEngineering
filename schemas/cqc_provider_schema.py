from pyspark.sql.types import (
    StructField,
    StructType,
    StringType,
    ArrayType,
    FloatType,
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
        StructField(NewColNames.provider_id, StringType(), True),
        StructField(NewColNames.location_ids, ArrayType(StringType(), True), True),
        StructField(NewColNames.organisation_type, StringType(), True),
        StructField(NewColNames.ownership_type, StringType(), True),
        StructField(NewColNames.type, StringType(), True),
        StructField(NewColNames.name, StringType(), True),
        StructField(NewColNames.brand_id, StringType(), True),
        StructField(NewColNames.brand_name, StringType(), True),
        StructField(NewColNames.ods_code, StringType(), True),
        StructField(NewColNames.registration_status, StringType(), True),
        StructField(NewColNames.registration_date, StringType(), True),
        StructField(NewColNames.companies_house_number, StringType(), True),
        StructField(NewColNames.charity_number, StringType(), True),
        StructField(NewColNames.website, StringType(), True),
        StructField(NewColNames.postal_address_line1, StringType(), True),
        StructField(NewColNames.postal_address_line2, StringType(), True),
        StructField(NewColNames.postal_address_town_city, StringType(), True),
        StructField(NewColNames.postal_address_county, StringType(), True),
        StructField(NewColNames.region, StringType(), True),
        StructField(NewColNames.postal_code, StringType(), True),
        StructField(NewColNames.also_known_as, StringType(), True),
        StructField(NewColNames.deregistration_date, StringType(), True),
        StructField(NewColNames.uprn, StringType(), True),
        StructField(NewColNames.onspd_latitude, StringType(), True),
        StructField(NewColNames.onspd_longitude, StringType(), True),
        StructField(NewColNames.onspd_icb_code, StringType(), True),
        StructField(NewColNames.onspd_icb_name, StringType(), True),
        StructField(NewColNames.main_phone_number, StringType(), True),
        StructField(NewColNames.inspection_directorate, StringType(), True),
        StructField(NewColNames.constituency, StringType(), True),
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
            NewColNames.contacts,
            ArrayType(
                StructType(
                    [
                        StructField(NewColNames.person_title, StringType(), True),
                        StructField(NewColNames.person_given_name, StringType(), True),
                        StructField(NewColNames.person_family_name, StringType(), True),
                        StructField(
                            NewColNames.person_roles,
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
            NewColNames.relationships,
            ArrayType(
                StructType(
                    [
                        StructField(
                            NewColNames.related_provider_id, StringType(), True
                        ),
                        StructField(
                            NewColNames.related_provider_name, StringType(), True
                        ),
                        StructField(NewColNames.type, StringType(), True),
                        StructField(NewColNames.reason, StringType(), True),
                    ]
                ),
                True,
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
                            NewColNames.nominated_individual,
                            StructType(
                                [
                                    StructField(
                                        NewColNames.person_title, StringType(), True
                                    ),
                                    StructField(
                                        NewColNames.person_given_name,
                                        StringType(),
                                        True,
                                    ),
                                    StructField(
                                        NewColNames.person_family_name,
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
            NewColNames.inspection_categories,
            ArrayType(
                StructType(
                    [
                        StructField(NewColNames.code, StringType(), True),
                        StructField(NewColNames.primary, StringType(), True),
                        StructField(NewColNames.name, StringType(), True),
                    ]
                ),
                True,
            ),
            True,
        ),
        StructField(
            NewColNames.inspection_areas,
            ArrayType(
                StructType(
                    [
                        StructField(NewColNames.inspection_area_id, StringType(), True),
                        StructField(
                            NewColNames.inspection_area_name, StringType(), True
                        ),
                        StructField(
                            NewColNames.inspection_area_type, StringType(), True
                        ),
                        StructField(NewColNames.status, StringType(), True),
                        StructField(NewColNames.end_date, StringType(), True),
                        StructField(
                            NewColNames.superseded_by,
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
                                    NewColNames.use_of_resources,
                                    StructType(
                                        [
                                            StructField(
                                                NewColNames.use_of_resources_summary,
                                                StringType(),
                                                True,
                                            ),
                                            StructField(
                                                NewColNames.use_of_resources_rating,
                                                StringType(),
                                                True,
                                            ),
                                            StructField(
                                                NewColNames.combined_quality_summary,
                                                StringType(),
                                                True,
                                            ),
                                            StructField(
                                                NewColNames.combined_quality_rating,
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
                                                    NewColNames.organisation_id,
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
                            ]
                        ),
                        True,
                    ),
                    StructField(
                        NewColNames.service_ratings,
                        ArrayType(
                            StructType(
                                [
                                    StructField(NewColNames.name, StringType(), True),
                                    StructField(NewColNames.rating, StringType(), True),
                                    StructField(
                                        NewColNames.report_date, StringType(), True
                                    ),
                                    StructField(
                                        NewColNames.organisation_id, StringType(), True
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
                        True,
                    ),
                ]
            ),
            True,
        ),
        StructField(
            NewColNames.historic_ratings,
            ArrayType(
                StructType(
                    [
                        StructField(NewColNames.report_date, StringType(), True),
                        StructField(NewColNames.report_link_id, StringType(), True),
                        StructField(NewColNames.organisation_id, StringType(), True),
                        StructField(
                            NewColNames.overall,
                            StructType(
                                [
                                    StructField(NewColNames.rating, StringType(), True),
                                    StructField(
                                        NewColNames.use_of_resources,
                                        StructType(
                                            [
                                                StructField(
                                                    NewColNames.use_of_resources_summary,
                                                    StringType(),
                                                    True,
                                                ),
                                                StructField(
                                                    NewColNames.use_of_resources_rating,
                                                    StringType(),
                                                    True,
                                                ),
                                                StructField(
                                                    NewColNames.combined_quality_summary,
                                                    StringType(),
                                                    True,
                                                ),
                                                StructField(
                                                    NewColNames.combined_quality_rating,
                                                    StringType(),
                                                    True,
                                                ),
                                            ]
                                        ),
                                        True,
                                    ),
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
                        StructField(
                            NewColNames.service_ratings,
                            ArrayType(
                                StructType(
                                    [
                                        StructField(
                                            NewColNames.name, StringType(), True
                                        ),
                                        StructField(
                                            NewColNames.rating, StringType(), True
                                        ),
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
                            True,
                        ),
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
                        StructField(NewColNames.link_id, StringType(), True),
                        StructField(NewColNames.report_date, StringType(), True),
                        StructField(NewColNames.first_visit_date, StringType(), True),
                        StructField(NewColNames.report_uri, StringType(), True),
                        StructField(NewColNames.report_type, StringType(), True),
                        StructField(
                            NewColNames.inspection_locations,
                            ArrayType(
                                StructType(
                                    [
                                        StructField(
                                            NewColNames.location_id, StringType(), True
                                        )
                                    ]
                                ),
                                True,
                            ),
                            True,
                        ),
                        StructField(
                            NewColNames.related_documents,
                            ArrayType(
                                StructType(
                                    [
                                        StructField(
                                            NewColNames.document_uri, StringType(), True
                                        ),
                                        StructField(
                                            NewColNames.document_type,
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
            NewColNames.unpublished_reports,
            ArrayType(
                StructType(
                    [StructField(NewColNames.first_visit_date, StringType(), True)]
                ),
                True,
            ),
            True,
        ),
    ]
)
