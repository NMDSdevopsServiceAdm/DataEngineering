from dataclasses import dataclass


@dataclass
class PublicationColumns:
    ct_total_employed_imputed: str = "ct_total_employed_imputed"
    ct_has_data_long_term: str = "ct_has_data_long_term"
    ct_has_data_medium_term: str = "ct_has_data_medium_term"
    ct_has_data_short_term: str = "ct_has_data_short_term"
    consistent_service: str = "consistent_service"
    ct_dispersion_filter_long_term: str = "ct_dispersion_filter_long_term"
    ct_dispersion_filter_medium_term: str = "ct_dispersion_filter_medium_term"
    ct_dispersion_filter_short_term: str = "ct_dispersion_filter_short_term"
    publication_filled_posts: str = "publication_filled_posts"
    publication_locationid_count: str = "publication_locationid_count"
    assessment_filled_posts_long_term: str = "assessment_filled_posts_long_term"
    assessment_filled_posts_medium_term: str = "assessment_filled_posts_medium_term"
    assessment_filled_posts_short_term: str = "assessment_filled_posts_short_term"
    assessment_locationid_count_long_term: str = "assessment_locationid_count_long_term"
    assessment_locationid_count_medium_term: str = (
        "assessment_locationid_count_medium_term"
    )
    assessment_locationid_count_short_term: str = (
        "assessment_locationid_count_short_term"
    )
    assessment_ct_total_employed_long_term: str = (
        "assessment_ct_total_employed_long_term"
    )
    assessment_ct_total_employed_medium_term: str = (
        "assessment_ct_total_employed_medium_term"
    )
    assessment_ct_total_employed_short_term: str = (
        "assessment_ct_total_employed_short_term"
    )
