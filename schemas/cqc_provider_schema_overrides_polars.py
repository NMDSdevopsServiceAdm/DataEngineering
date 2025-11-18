import polars as pl

from utils.column_names.raw_data_files.cqc_provider_api_columns import (
    CqcProviderApiColumns as ColNames,
)

POLARS_PROVIDER_SCHEMA_OVERRIDES = {
    ColNames.onspd_latitude: pl.Utf8,
    ColNames.onspd_longitude: pl.Utf8,
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
    ColNames.also_known_as: pl.Utf8,
}
