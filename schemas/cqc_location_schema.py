from pyspark.sql.types import (
    StructField,
    StructType,
    StringType,
    ArrayType,
    IntegerType,
)

from utils.column_names.raw_data_files.cqc_location_api_columns import (
    NewCqcLocationApiColumns as NewColNames,
)

"""
This schema (and the data in s3) has been updated to match changes which were announced 
to be taking place on 30/1/25. The three new columns added are highlighted below. This 
update to the API has now been postponed and no new launch date has been communicated as
yet. We are leaving these columns in the schema for now, as they do not affect the pipeline.
If another API schema change happens before these columns are added, they may need removing.

The changes were made in PR #680: Update location api schema. Note that reversing this PR will 
also roll back the version number for the CQC locations data in s3. The data in s3 and the
version number in s3 will need reverting separately.
"""

LOCATION_SCHEMA = StructType(
    [
        StructField(NewColNames.location_id, StringType(), True),
        StructField(NewColNames.provider_id, StringType(), True),
        StructField(NewColNames.organisation_type, StringType(), True),
        StructField(NewColNames.type, StringType(), True),
        StructField(NewColNames.name, StringType(), True),
        StructField(NewColNames.brand_id, StringType(), True),
        StructField(NewColNames.brand_name, StringType(), True),
        StructField(NewColNames.onspd_ccg_code, StringType(), True),
        StructField(NewColNames.onspd_ccg_name, StringType(), True),
        StructField(NewColNames.ods_ccg_code, StringType(), True),
        StructField(NewColNames.ods_ccg_name, StringType(), True),
        StructField(NewColNames.onspd_icb_code, StringType(), True),
        StructField(NewColNames.onspd_icb_name, StringType(), True),
        StructField(NewColNames.ods_code, StringType(), True),
        StructField(NewColNames.registration_status, StringType(), True),
        StructField(NewColNames.registration_date, StringType(), True),
        StructField(NewColNames.deregistration_date, StringType(), True),
        StructField(NewColNames.dormancy, StringType(), True),
        StructField(NewColNames.dormancy_start_date, StringType(), True),
        StructField(NewColNames.dormancy_end_date, StringType(), True),
        StructField(NewColNames.also_known_as, StringType(), True),
        StructField(NewColNames.onspd_latitude, StringType(), True),
        StructField(NewColNames.onspd_longitude, StringType(), True),
        StructField(NewColNames.care_home, StringType(), True),
        StructField(NewColNames.inspection_directorate, StringType(), True),
        StructField(NewColNames.website, StringType(), True),
        StructField(NewColNames.postal_address_line1, StringType(), True),
        StructField(NewColNames.postal_address_line2, StringType(), True),
        StructField(NewColNames.postal_address_town_city, StringType(), True),
        StructField(NewColNames.postal_address_county, StringType(), True),
        StructField(NewColNames.region, StringType(), True),
        StructField(NewColNames.postal_code, StringType(), True),
        StructField(NewColNames.uprn, StringType(), True),
        StructField(NewColNames.main_phone_number, StringType(), True),
        StructField(NewColNames.registered_manager_absent_date, StringType(), True),
        StructField(NewColNames.number_of_beds, IntegerType(), True),
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
                ),
                True,
            ),
            True,
        ),
        StructField(
            NewColNames.location_types,
            ArrayType(
                StructType([StructField(NewColNames.type, StringType(), True)]), True
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
                                            NewColNames.person_family_name,
                                            StringType(),
                                            True,
                                        ),
                                        StructField(
                                            NewColNames.person_given_name,
                                            StringType(),
                                            True,
                                        ),
                                        StructField(
                                            NewColNames.person_roles,
                                            ArrayType(StringType(), True),
                                            True,
                                        ),
                                        StructField(
                                            NewColNames.person_title, StringType(), True
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
            NewColNames.gac_service_types,
            ArrayType(
                StructType(
                    [
                        StructField(NewColNames.name, StringType(), True),
                        StructField(NewColNames.description, StringType(), True),
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
        StructField(
            NewColNames.inspection_categories,
            ArrayType(
                StructType(
                    [
                        StructField(NewColNames.primary, StringType(), True),
                        StructField(NewColNames.code, StringType(), True),
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
                        StructField(
                            NewColNames.superseded_by,
                            ArrayType(StringType(), True),
                            True,
                        ),
                        StructField(NewColNames.end_date, StringType(), True),
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
                                                NewColNames.organisation_id,
                                                StringType(),
                                                True,
                                            ),
                                            StructField(
                                                NewColNames.summary, StringType(), True
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
                                                    NewColNames.use_of_resources_rating,
                                                    StringType(),
                                                    True,
                                                ),
                                                StructField(
                                                    NewColNames.use_of_resources_summary,
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
                            NewColNames.related_documents,
                            ArrayType(
                                StructType(
                                    [
                                        StructField(
                                            NewColNames.document_type,
                                            StringType(),
                                            True,
                                        ),
                                        StructField(
                                            NewColNames.document_uri, StringType(), True
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
        StructField(
            NewColNames.provider_inspection_areas,
            ArrayType(
                StructType(
                    [
                        StructField(NewColNames.inspection_area_id, StringType(), True),
                        StructField(
                            NewColNames.reports,
                            ArrayType(
                                StructType(
                                    [
                                        StructField(
                                            NewColNames.inspection_id,
                                            StringType(),
                                            True,
                                        ),
                                        StructField(
                                            NewColNames.report_link_id,
                                            StringType(),
                                            True,
                                        ),
                                        StructField(
                                            NewColNames.provider_id, StringType(), True
                                        ),
                                        StructField(
                                            NewColNames.location_id, StringType(), True
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
            NewColNames.specialism,  # new column
            ArrayType(
                StructType(
                    [
                        StructField(NewColNames.code, StringType(), True),
                        StructField(NewColNames.name, StringType(), True),
                    ]
                ),
                True,
            ),
            True,
        ),
        StructField(
            NewColNames.age_group,  # new column
            ArrayType(
                StructType(
                    [
                        StructField(NewColNames.code, StringType(), True),
                        StructField(NewColNames.name, StringType(), True),
                    ]
                ),
                True,
            ),
            True,
        ),
        StructField(
            NewColNames.setting_services,  # new column
            ArrayType(
                StructType(
                    [
                        StructField(NewColNames.setting_type_code, StringType(), True),
                        StructField(NewColNames.setting_type_name, StringType(), True),
                        StructField(NewColNames.service_type_code, StringType(), True),
                        StructField(NewColNames.service_type_name, StringType(), True),
                    ]
                ),
                True,
            ),
            True,
        ),
    ]
)
