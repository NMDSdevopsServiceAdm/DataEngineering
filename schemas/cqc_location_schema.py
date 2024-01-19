from pyspark.sql.types import (
    StructField,
    StructType,
    StringType,
    ArrayType,
    FloatType,
    IntegerType,
)

from utils.ind_cqc_column_names.cqc_location_api_columns import (
    CqcLocationApiColumns as ColNames,
)

LOCATION_SCHEMA = StructType(
    fields=[
        StructField(ColNames.location_id, StringType(), True),
        StructField(ColNames.provider_id, StringType(), True),
        StructField(ColNames.organisation_type, StringType(), True),
        StructField(ColNames.type, StringType(), True),
        StructField(ColNames.name, StringType(), True),
        StructField(ColNames.ccg_code, StringType(), True),
        StructField(ColNames.ccg_name, StringType(), True),
        StructField(ColNames.ods_code, StringType(), True),
        StructField(ColNames.uprn, StringType(), True),
        StructField(ColNames.registration_status, StringType(), True),
        StructField(ColNames.registration_date, StringType(), True),
        StructField(ColNames.deregistration_date, StringType(), True),
        StructField(ColNames.dormancy, StringType(), True),
        StructField(ColNames.number_of_beds, IntegerType(), True),
        StructField(ColNames.website, StringType(), True),
        StructField(ColNames.address_line_one, StringType(), True),
        StructField(ColNames.town_or_city, StringType(), True),
        StructField(ColNames.county, StringType(), True),
        StructField(ColNames.region, StringType(), True),
        StructField(ColNames.postcode, StringType(), True),
        StructField(ColNames.latitude, FloatType(), True),
        StructField(ColNames.longitude, FloatType(), True),
        StructField(ColNames.care_home, StringType(), True),
        StructField(ColNames.inspection_directorate, StringType(), True),
        StructField(ColNames.phone_number, StringType(), True),
        StructField(ColNames.constituancy, StringType(), True),
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
            ColNames.relationships,
            ArrayType(
                StructType(
                    [
                        StructField(ColNames.related_location_id, StringType(), True),
                        StructField(ColNames.related_location_name, StringType(), True),
                        StructField(ColNames.type, StringType(), True),
                        StructField(ColNames.reason, StringType(), True),
                    ]
                )
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
                            ColNames.contacts,
                            ArrayType(
                                StructType(
                                    [
                                        StructField(ColNames.title, StringType(), True),
                                        StructField(
                                            ColNames.given_name, StringType(), True
                                        ),
                                        StructField(
                                            ColNames.family_name, StringType(), True
                                        ),
                                        StructField(ColNames.roles, StringType(), True),
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
            ColNames.gac_service_types,
            ArrayType(
                StructType(
                    [
                        StructField(ColNames.name, StringType(), True),
                        StructField(ColNames.description, StringType(), True),
                    ]
                )
            ),
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
                )
            ),
            True,
        ),
        StructField(
            ColNames.specialisms,
            ArrayType(
                StructType(
                    [
                        StructField(ColNames.name, StringType(), True),
                    ]
                )
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
                                StructField(ColNames.report_date, StringType(), True),
                                StructField(
                                    ColNames.report_link_id, StringType(), True
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
                                                    ColNames.rating, StringType(), True
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
            ColNames.historic_ratings,
            ArrayType(
                StructType(
                    [
                        StructField(ColNames.organisation_id, StringType(), True),
                        StructField(ColNames.report_link_id, StringType(), True),
                        StructField(ColNames.report_date, StringType(), True),
                        StructField(
                            ColNames.overall,
                            StructType(
                                [
                                    StructField(ColNames.rating, StringType(), True),
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
                    ]
                ),
                True,
            ),
        ),
        StructField(
            ColNames.reports,
            ArrayType(
                StructType(
                    [
                        StructField(ColNames.link_id, StringType(), True),
                        StructField(ColNames.report_date, StringType(), True),
                        StructField(ColNames.report_uri, StringType(), True),
                        StructField(ColNames.first_visit_date, StringType(), True),
                        StructField(ColNames.report_type, StringType(), True),
                    ]
                )
            ),
            True,
        ),
    ]
)
