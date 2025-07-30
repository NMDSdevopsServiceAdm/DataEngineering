import polars as pl
from utils.column_names.raw_data_files.cqc_provider_api_columns import (
    CqcProviderApiColumns as ColNames,
)


POLARS_PROVIDER_SCHEMA = {
    ColNames.provider_id: pl.Utf8,
    ColNames.location_ids: pl.List(pl.Utf8),
    ColNames.organisation_type: pl.Utf8,
    ColNames.ownership_type: pl.Utf8,
    ColNames.type: pl.Utf8,
    ColNames.name: pl.Utf8,
    ColNames.brand_id: pl.Utf8,
    ColNames.brand_name: pl.Utf8,
    ColNames.ods_code: pl.Utf8,
    ColNames.registration_status: pl.Utf8,
    ColNames.registration_date: pl.Utf8,
    ColNames.companies_house_number: pl.Utf8,
    ColNames.charity_number: pl.Utf8,
    ColNames.website: pl.Utf8,
    ColNames.postal_address_line1: pl.Utf8,
    ColNames.postal_address_line2: pl.Utf8,
    ColNames.postal_address_town_city: pl.Utf8,
    ColNames.postal_address_county: pl.Utf8,
    ColNames.region: pl.Utf8,
    ColNames.postal_code: pl.Utf8,
    ColNames.also_known_as: pl.Utf8,
    ColNames.deregistration_date: pl.Utf8,
    ColNames.uprn: pl.Utf8,
    ColNames.onspd_latitude: pl.Utf8,
    ColNames.onspd_longitude: pl.Utf8,
    ColNames.onspd_icb_code: pl.Utf8,
    ColNames.onspd_icb_name: pl.Utf8,
    ColNames.main_phone_number: pl.Utf8,
    ColNames.inspection_directorate: pl.Utf8,
    ColNames.constituency: pl.Utf8,
    ColNames.local_authority: pl.Utf8,
    ColNames.last_inspection: pl.Struct({ColNames.date: pl.Utf8}),
    ColNames.last_report: pl.Struct({ColNames.publication_date: pl.Utf8}),
    ColNames.contacts: pl.List(
        pl.Struct(
            {
                ColNames.person_title: pl.Utf8,
                ColNames.person_given_name: pl.Utf8,
                ColNames.person_family_name: pl.Utf8,
                ColNames.person_roles: pl.List(pl.Utf8),
            }
        )
    ),
    ColNames.relationships: pl.List(
        pl.Struct(
            {
                ColNames.related_provider_id: pl.Utf8,
                ColNames.related_provider_name: pl.Utf8,
                ColNames.type: pl.Utf8,
                ColNames.reason: pl.Utf8,
            }
        )
    ),
    ColNames.regulated_activities: pl.List(
        pl.Struct(
            {
                ColNames.name: pl.Utf8,
                ColNames.code: pl.Utf8,
                ColNames.nominated_individual: pl.Struct(
                    {
                        ColNames.person_title: pl.Utf8,
                        ColNames.person_given_name: pl.Utf8,
                        ColNames.person_family_name: pl.Utf8,
                    }
                ),
            }
        )
    ),
    ColNames.inspection_categories: pl.List(
        pl.Struct(
            {
                ColNames.code: pl.Utf8,
                ColNames.primary: pl.Utf8,
                ColNames.name: pl.Utf8,
            }
        )
    ),
    ColNames.inspection_areas: pl.List(
        pl.Struct(
            {
                ColNames.inspection_area_id: pl.Utf8,
                ColNames.inspection_area_name: pl.Utf8,
                ColNames.inspection_area_type: pl.Utf8,
                ColNames.status: pl.Utf8,
                ColNames.end_date: pl.Utf8,
                ColNames.superseded_by: pl.List(pl.Utf8),
            }
        )
    ),
    ColNames.current_ratings: pl.Struct(
        {
            ColNames.overall: pl.Struct(
                {
                    ColNames.rating: pl.Utf8,
                    ColNames.report_date: pl.Utf8,
                    ColNames.report_link_id: pl.Utf8,
                    ColNames.use_of_resources: pl.Struct(
                        {
                            ColNames.use_of_resources_summary: pl.Utf8,
                            ColNames.use_of_resources_rating: pl.Utf8,
                            ColNames.combined_quality_summary: pl.Utf8,
                            ColNames.combined_quality_rating: pl.Utf8,
                            ColNames.report_date: pl.Utf8,
                            ColNames.report_link_id: pl.Utf8,
                        }
                    ),
                    ColNames.key_question_ratings: pl.List(
                        pl.Struct(
                            {
                                ColNames.name: pl.Utf8,
                                ColNames.rating: pl.Utf8,
                                ColNames.report_date: pl.Utf8,
                                ColNames.organisation_id: pl.Utf8,
                                ColNames.report_link_id: pl.Utf8,
                            }
                        )
                    ),
                }
            ),
            ColNames.service_ratings: pl.List(
                pl.Struct(
                    {
                        ColNames.name: pl.Utf8,
                        ColNames.rating: pl.Utf8,
                        ColNames.report_date: pl.Utf8,
                        ColNames.organisation_id: pl.Utf8,
                        ColNames.report_link_id: pl.Utf8,
                        ColNames.key_question_ratings: pl.List(
                            pl.Struct(
                                {
                                    ColNames.name: pl.Utf8,
                                    ColNames.rating: pl.Utf8,
                                }
                            )
                        ),
                    }
                )
            ),
        }
    ),
    ColNames.historic_ratings: pl.List(
        pl.Struct(
            {
                ColNames.report_date: pl.Utf8,
                ColNames.report_link_id: pl.Utf8,
                ColNames.organisation_id: pl.Utf8,
                ColNames.overall: pl.Struct(
                    {
                        ColNames.rating: pl.Utf8,
                        ColNames.use_of_resources: pl.Struct(
                            {
                                ColNames.use_of_resources_summary: pl.Utf8,
                                ColNames.use_of_resources_rating: pl.Utf8,
                                ColNames.combined_quality_summary: pl.Utf8,
                                ColNames.combined_quality_rating: pl.Utf8,
                            }
                        ),
                        ColNames.key_question_ratings: pl.List(
                            pl.Struct(
                                {
                                    ColNames.name: pl.Utf8,
                                    ColNames.rating: pl.Utf8,
                                }
                            )
                        ),
                    }
                ),
                ColNames.service_ratings: pl.List(
                    pl.Struct(
                        {
                            ColNames.name: pl.Utf8,
                            ColNames.rating: pl.Utf8,
                            ColNames.key_question_ratings: pl.List(
                                pl.Struct(
                                    {
                                        ColNames.name: pl.Utf8,
                                        ColNames.rating: pl.Utf8,
                                    }
                                )
                            ),
                        }
                    )
                ),
            }
        )
    ),
    ColNames.reports: pl.List(
        pl.Struct(
            {
                ColNames.link_id: pl.Utf8,
                ColNames.report_date: pl.Utf8,
                ColNames.first_visit_date: pl.Utf8,
                ColNames.report_uri: pl.Utf8,
                ColNames.report_type: pl.Utf8,
                ColNames.inspection_locations: pl.List(
                    pl.Struct({ColNames.location_id: pl.Utf8})
                ),
                ColNames.related_documents: pl.List(
                    pl.Struct(
                        {
                            ColNames.document_uri: pl.Utf8,
                            ColNames.document_type: pl.Utf8,
                        }
                    )
                ),
            }
        )
    ),
    ColNames.unpublished_reports: pl.List(
        pl.Struct({ColNames.first_visit_date: pl.Utf8})
    ),
}