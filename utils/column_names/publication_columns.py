from dataclasses import dataclass


@dataclass
class PublicationColumns:
    run_number: str = "run_number"
    ct_total_employed_imputed: str = "ct_total_employed_imputed"
    ct_has_data_long_term: str = "ct_has_data_long_term"
    ct_has_data_medium_term: str = "ct_has_data_medium_term"
    ct_has_data_short_term: str = "ct_has_data_short_term"
    consistent_service_long_term: str = "consistent_service_long_term"
    consistent_service_medium_term: str = "consistent_service_medium_term"
    consistent_service_short_term: str = "consistent_service_short_term"
    ct_dispersion_filter_long_term: str = "ct_dispersion_filter_long_term"
    ct_dispersion_filter_medium_term: str = "ct_dispersion_filter_medium_term"
    ct_dispersion_filter_short_term: str = "ct_dispersion_filter_short_term"
    publication_filled_posts: str = "publication_filled_posts"
    publication_filled_posts_rounded: str = "publication_filled_posts_rounded"
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
    assessment_ct_total_employed_long_term_no_filter: str = (
        "assessment_ct_total_employed_long_term_no_filter"
    )
    assessment_ct_cumulative_perc_change_long_term: str = (
        "assessment_ct_cumulative_perc_change_long_term"
    )
    assessment_ct_cumulative_perc_change_medium_term: str = (
        "assessment_ct_cumulative_perc_change_medium_term"
    )
    assessment_ct_cumulative_perc_change_short_term: str = (
        "assessment_ct_cumulative_perc_change_short_term"
    )
    assessment_ct_total_employed_cumulative_perc_change_long_term_no_filter: str = (
        "assessment_ct_total_employed_cumulative_perc_change_long_term_no_filter"
    )
    assessment_filled_posts_cumulative_perc_change_long_term: str = (
        "assessment_filled_posts_cumulative_perc_change_long_term"
    )
    publication_filled_posts_cumulative_perc_change_short_term: str = (
        "publication_filled_posts_cumulative_perc_change_short_term"
    )
    publication_locationid_count_cumulative_perc_change_short_term: str = (
        "publication_locationid_count_cumulative_perc_change_short_term"
    )
    publication_filled_posts_april_perc_change: str = (
        "publication_filled_posts_april_perc_change"
    )
    publication_locationid_count_april_perc_change: str = (
        "publication_locationid_count_april_perc_change"
    )
    main_service: str = "Main service"
    job_group: str = "Job group"
    region: str = "Region"
    estimated_filled_posts: str = "Estimated filled posts"
    cqc_locations: str = "CQC locations"
    annual_percentage_change: str = "Annual percentage change"
    monthly_percentage_change: str = "Monthly percentage change"
    period: str = "Period"
    period_label: str = "Period label"
    estimated_filled_posts_rounded: str = "Estimated filled posts (rounded)"
