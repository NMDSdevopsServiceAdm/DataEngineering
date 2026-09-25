from dataclasses import dataclass


# Ticket 2110 spike only. Kept in the stage rather than utils/column_names so that
# spike pushes rebuild only the _03_independent_cqc image (every image copies
# utils/column_names). Move to utils/column_names if this is ever built for real.
@dataclass
class EmploymentStatusSpikeColumns:
    """Columns for the ticket 2110 L1/L2 employment status imputation spike."""

    employment_status_label: str = "employment_status_label"
    employment_status_rate: str = "employment_status_rate"
    employment_status_rolling_ratio: str = "employment_status_rolling_ratio"
    imputed_employment_status_rate: str = "imputed_employment_status_rate"
    first_known_date: str = "es_spike_first_known_date"
    last_known_date: str = "es_spike_last_known_date"
    first_known_value: str = "es_spike_first_known_value"
    last_known_value: str = "es_spike_last_known_value"
    previous_known_date: str = "es_spike_previous_known_date"
    next_known_date: str = "es_spike_next_known_date"
    ratio_total: str = "es_spike_ratio_total"
    contributing_rows: str = "es_spike_contributing_rows"
    imputed_rate_for_trendline: str = "es_spike_imputed_rate_for_trendline"
    unnormalised_rate: str = "es_spike_unnormalised_rate"
