import polars as pl

from utils.column_names.raw_data_files.cqc_location_api_columns import (
    NewCqcLocationApiColumns as NewColNames,
)

POLARS_LOCATION_SCHEMA = pl.Schema(
    [
        (NewColNames.location_id, pl.String()),
        (NewColNames.provider_id, pl.String()),
        (NewColNames.organisation_type, pl.String()),
        (NewColNames.type, pl.String()),
        (NewColNames.name, pl.String()),
        (NewColNames.brand_id, pl.String()),
        (NewColNames.brand_name, pl.String()),
        (NewColNames.onspd_ccg_code, pl.String()),
        (NewColNames.onspd_ccg_name, pl.String()),
        (NewColNames.ods_ccg_code, pl.String()),
        (NewColNames.ods_ccg_name, pl.String()),
        (NewColNames.onspd_icb_code, pl.String()),
        (NewColNames.onspd_icb_name, pl.String()),
        (NewColNames.ods_code, pl.String()),
        (NewColNames.registration_status, pl.String()),
        (NewColNames.registration_date, pl.Date()),
        (NewColNames.deregistration_date, pl.Date()),
        (NewColNames.dormancy, pl.String()),
        (NewColNames.dormancy_start_date, pl.String()),
        (NewColNames.dormancy_end_date, pl.String()),
        (NewColNames.also_known_as, pl.String()),
        (NewColNames.onspd_latitude, pl.String()),
        (NewColNames.onspd_longitude, pl.String()),
        (NewColNames.care_home, pl.String()),
        (NewColNames.inspection_directorate, pl.String()),
        (NewColNames.website, pl.String()),
        (NewColNames.postal_address_line1, pl.String()),
        (NewColNames.postal_address_line2, pl.String()),
        (NewColNames.postal_address_town_city, pl.String()),
        (NewColNames.postal_address_county, pl.String()),
        (NewColNames.region, pl.String()),
        (NewColNames.postal_code, pl.String()),
        (NewColNames.uprn, pl.String()),
        (NewColNames.main_phone_number, pl.String()),
        (NewColNames.registered_manager_absent_date, pl.String()),
        (NewColNames.number_of_beds, pl.Int32()),
        (NewColNames.constituency, pl.String()),
        (NewColNames.local_authority, pl.String()),
        (NewColNames.last_inspection, pl.Struct({NewColNames.date: pl.String()})),
        (
            NewColNames.last_report,
            pl.Struct({NewColNames.publication_date: pl.String()}),
        ),
        (
            NewColNames.relationships,
            pl.List(
                pl.Struct(
                    {
                        NewColNames.related_location_id: pl.String(),
                        NewColNames.related_location_name: pl.String(),
                        NewColNames.type: pl.String(),
                        NewColNames.reason: pl.String(),
                    }
                )
            ),
        ),
        (
            NewColNames.location_types,
            pl.List(pl.Struct({NewColNames.type: pl.String()})),
        ),
        (
            NewColNames.regulated_activities,
            pl.List(
                pl.Struct(
                    {
                        NewColNames.name: pl.String(),
                        NewColNames.code: pl.String(),
                        NewColNames.contacts: pl.List(
                            pl.Struct(
                                {
                                    NewColNames.person_family_name: pl.String(),
                                    NewColNames.person_given_name: pl.String(),
                                    NewColNames.person_roles: pl.List(pl.String()),
                                    NewColNames.person_title: pl.String(),
                                }
                            )
                        ),
                    }
                )
            ),
        ),
        (
            NewColNames.gac_service_types,
            pl.List(
                pl.Struct(
                    {
                        NewColNames.name: pl.String(),
                        NewColNames.description: pl.String(),
                    }
                )
            ),
        ),
        (NewColNames.specialisms, pl.List(pl.Struct({NewColNames.name: pl.String()}))),
        (
            NewColNames.inspection_categories,
            pl.List(
                pl.Struct(
                    {
                        NewColNames.primary: pl.String(),
                        NewColNames.code: pl.String(),
                        NewColNames.name: pl.String(),
                    }
                )
            ),
        ),
        (
            NewColNames.inspection_areas,
            pl.List(
                pl.Struct(
                    {
                        NewColNames.inspection_area_id: pl.String(),
                        NewColNames.inspection_area_name: pl.String(),
                        NewColNames.inspection_area_type: pl.String(),
                        NewColNames.status: pl.String(),
                        NewColNames.superseded_by: pl.List(pl.String()),
                        NewColNames.end_date: pl.String(),
                    }
                )
            ),
        ),
        (
            NewColNames.current_ratings,
            pl.Struct(
                {
                    NewColNames.overall: pl.Struct(
                        {
                            NewColNames.organisation_id: pl.String(),
                            NewColNames.rating: pl.String(),
                            NewColNames.report_date: pl.String(),
                            NewColNames.report_link_id: pl.String(),
                            NewColNames.use_of_resources: pl.Struct(
                                {
                                    NewColNames.organisation_id: pl.String(),
                                    NewColNames.summary: pl.String(),
                                    NewColNames.use_of_resources_rating: pl.String(),
                                    NewColNames.combined_quality_summary: pl.String(),
                                    NewColNames.combined_quality_rating: pl.String(),
                                    NewColNames.report_date: pl.String(),
                                    NewColNames.report_link_id: pl.String(),
                                }
                            ),
                            NewColNames.key_question_ratings: pl.List(
                                pl.Struct(
                                    {
                                        NewColNames.name: pl.String(),
                                        NewColNames.rating: pl.String(),
                                        NewColNames.report_date: pl.String(),
                                        NewColNames.organisation_id: pl.String(),
                                        NewColNames.report_link_id: pl.String(),
                                    }
                                )
                            ),
                        }
                    ),
                    NewColNames.service_ratings: pl.List(
                        pl.Struct(
                            {
                                NewColNames.name: pl.String(),
                                NewColNames.rating: pl.String(),
                                NewColNames.report_date: pl.String(),
                                NewColNames.organisation_id: pl.String(),
                                NewColNames.report_link_id: pl.String(),
                                NewColNames.key_question_ratings: pl.List(
                                    pl.Struct(
                                        {
                                            NewColNames.name: pl.String(),
                                            NewColNames.rating: pl.String(),
                                        }
                                    )
                                ),
                            }
                        )
                    ),
                }
            ),
        ),
        (
            NewColNames.historic_ratings,
            pl.List(
                pl.Struct(
                    {
                        NewColNames.report_date: pl.String(),
                        NewColNames.report_link_id: pl.String(),
                        NewColNames.organisation_id: pl.String(),
                        NewColNames.service_ratings: pl.List(
                            pl.Struct(
                                {
                                    NewColNames.name: pl.String(),
                                    NewColNames.rating: pl.String(),
                                    NewColNames.key_question_ratings: pl.List(
                                        pl.Struct(
                                            {
                                                NewColNames.name: pl.String(),
                                                NewColNames.rating: pl.String(),
                                            }
                                        )
                                    ),
                                }
                            )
                        ),
                        NewColNames.overall: pl.Struct(
                            {
                                NewColNames.rating: pl.String(),
                                NewColNames.use_of_resources: pl.Struct(
                                    {
                                        NewColNames.combined_quality_rating: pl.String(),
                                        NewColNames.combined_quality_summary: pl.String(),
                                        NewColNames.use_of_resources_rating: pl.String(),
                                        NewColNames.use_of_resources_summary: pl.String(),
                                    }
                                ),
                                NewColNames.key_question_ratings: pl.List(
                                    pl.Struct(
                                        {
                                            NewColNames.name: pl.String(),
                                            NewColNames.rating: pl.String(),
                                        }
                                    )
                                ),
                            }
                        ),
                    }
                )
            ),
        ),
        (
            NewColNames.reports,
            pl.List(
                pl.Struct(
                    {
                        NewColNames.link_id: pl.String(),
                        NewColNames.report_date: pl.String(),
                        NewColNames.first_visit_date: pl.String(),
                        NewColNames.report_uri: pl.String(),
                        NewColNames.report_type: pl.String(),
                        NewColNames.related_documents: pl.List(
                            pl.Struct(
                                {
                                    NewColNames.document_type: pl.String(),
                                    NewColNames.document_uri: pl.String(),
                                }
                            )
                        ),
                    }
                )
            ),
        ),
        (
            NewColNames.unpublished_reports,
            pl.List(pl.Struct({NewColNames.first_visit_date: pl.String()})),
        ),
        (
            NewColNames.provider_inspection_areas,
            pl.List(
                pl.Struct(
                    {
                        NewColNames.inspection_area_id: pl.String(),
                        NewColNames.reports: pl.List(
                            pl.Struct(
                                {
                                    NewColNames.inspection_id: pl.String(),
                                    NewColNames.report_link_id: pl.String(),
                                    NewColNames.provider_id: pl.String(),
                                    NewColNames.location_id: pl.String(),
                                }
                            )
                        ),
                    }
                )
            ),
        ),
        (
            NewColNames.specialism,
            pl.List(
                pl.Struct(
                    {NewColNames.code: pl.String(), NewColNames.name: pl.String()}
                )
            ),
        ),
        (
            NewColNames.age_group,
            pl.List(
                pl.Struct(
                    {NewColNames.code: pl.String(), NewColNames.name: pl.String()}
                )
            ),
        ),
        (
            NewColNames.setting_services,
            pl.List(
                pl.Struct(
                    {
                        NewColNames.setting_type_code: pl.String(),
                        NewColNames.setting_type_name: pl.String(),
                        NewColNames.service_type_code: pl.String(),
                        NewColNames.service_type_name: pl.String(),
                    }
                )
            ),
        ),
        (
            NewColNames.assessment,
            pl.List(
                pl.Struct(
                    {
                        NewColNames.assessment_plan_published_datetime: pl.String(),
                        NewColNames.ratings: pl.Struct(
                            {
                                NewColNames.overall: pl.List(
                                    pl.Struct(
                                        {
                                            NewColNames.rating: pl.String(),
                                            NewColNames.status: pl.String(),
                                            NewColNames.key_question_ratings: pl.List(
                                                pl.Struct(
                                                    {
                                                        NewColNames.name: pl.String(),
                                                        NewColNames.rating: pl.String(),
                                                        NewColNames.status: pl.String(),
                                                    }
                                                )
                                            ),
                                        }
                                    )
                                ),
                                NewColNames.asg_ratings: pl.List(
                                    pl.Struct(
                                        {
                                            NewColNames.assessment_plan_id: pl.String(),
                                            NewColNames.title: pl.String(),
                                            NewColNames.assessment_date: pl.String(),
                                            NewColNames.assessment_plan_status: pl.String(),
                                            NewColNames.name: pl.String(),
                                            NewColNames.rating: pl.String(),
                                            NewColNames.status: pl.String(),
                                            NewColNames.key_question_ratings: pl.List(
                                                pl.Struct(
                                                    {
                                                        NewColNames.name: pl.String(),
                                                        NewColNames.rating: pl.String(),
                                                        NewColNames.status: pl.String(),
                                                        NewColNames.percentage_score: pl.String(),
                                                    }
                                                )
                                            ),
                                        }
                                    )
                                ),
                            }
                        ),
                    }
                )
            ),
        ),
    ]
)
