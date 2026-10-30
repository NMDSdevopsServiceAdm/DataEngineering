from dataclasses import dataclass


@dataclass
class PublicationColumns:
    ct_total_employed_imputed: str = "ct_total_employed_imputed"
    ct_has_data_long: str = "ct_has_data_long"
    ct_has_data_medium: str = "ct_has_data_medium"
    ct_has_data_short: str = "ct_has_data_short"
    consistent_service_long: str = "consistent_service_long"
    consistent_service_medium: str = "consistent_service_medium"
    consistent_service_short: str = "consistent_service_short"
    ct_dispersion_filter_long: str = "ct_dispersion_filter_long"
    ct_dispersion_filter_medium: str = "ct_dispersion_filter_medium"
    ct_dispersion_filter_short: str = "ct_dispersion_filter_short"
    unfiltered_filled_posts: str = "unfiltered_filled_posts"
    unfiltered_filled_posts_rounded: str = "unfiltered_filled_posts_rounded"
    unfiltered_locationid_count: str = "unfiltered_locationid_count"
    filtered_long_sfc_filled_posts: str = "filtered_long_sfc_filled_posts"
    filtered_medium_sfc_filled_posts: str = "filtered_medium_sfc_filled_posts"
    filtered_short_sfc_filled_posts: str = "filtered_short_sfc_filled_posts"
    filtered_long_sfc_locationid_count: str = "filtered_long_sfc_locationid_count"
    filtered_medium_sfc_locationid_count: str = "filtered_medium_sfc_locationid_count"
    filtered_short_sfc_locationid_count: str = "filtered_short_sfc_locationid_count"
    filtered_long_ct_total_employed: str = "filtered_long_ct_total_employed"
    filtered_medium_ct_total_employed: str = "filtered_medium_ct_total_employed"
    filtered_short_ct_total_employed: str = "filtered_short_ct_total_employed"
    unfiltered_long_ct_total_employed: str = "unfiltered_long_ct_total_employed"
    filtered_long_ct_cum_pct_change: str = "filtered_long_ct_cum_pct_change"
    filtered_medium_ct_cum_pct_change: str = "filtered_medium_ct_cum_pct_change"
    filtered_short_ct_cum_pct_change: str = "filtered_short_ct_cum_pct_change"
    filtered_long_sfc_cum_pct_change: str = "filtered_long_sfc_cum_pct_change"
    filtered_medium_sfc_cum_pct_change: str = "filtered_medium_sfc_cum_pct_change"
    filtered_short_sfc_cum_pct_change: str = "filtered_short_sfc_cum_pct_change"
    unfiltered_long_ct_cum_pct_change: str = "unfiltered_long_ct_cum_pct_change"
    unfiltered_long_sfc_cum_pct_change: str = "unfiltered_long_sfc_cum_pct_change"
    unfiltered_short_sfc_cum_pct_change: str = "unfiltered_short_sfc_cum_pct_change"
    filtered_locationid_count_cum_perc_change_short: str = (
        "filtered_locationid_count_cum_perc_change_short"
    )
    prev_run_filled_posts: str = "prev_run_filled_posts"
    unfiltered_filled_posts_perc_change_vs_prev_run: str = (
        "unfiltered_filled_posts_perc_change_vs_prev_run"
    )
    unfiltered_filled_posts_april_perc_change: str = (
        "unfiltered_filled_posts_april_perc_change"
    )
    unfiltered_locationid_count_april_perc_change: str = (
        "unfiltered_locationid_count_april_perc_change"
    )
    main_service: str = "Main service"
    job_group: str = "Job group"
    region: str = "Region"
    estimated_filled_posts: str = "Estimated filled posts"
    cqc_locations: str = "CQC locations"
    annual_percentage_change: str = "Annual percentage change"
    monthly_percentage_change: str = "Monthly percentage change"
    period: str = "Period"
    period_label_short: str = "period_label_short"
    period_label_long: str = "period_label_long"
    estimated_filled_posts_rounded: str = "Estimated filled posts (rounded)"
