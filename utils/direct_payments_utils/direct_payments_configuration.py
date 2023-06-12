from dataclasses import dataclass


@dataclass
class DirectPaymentConfiguration:
    MOST_RECENT_YEAR: int = 2021
    # The carer's employing percentage was calculated from a question in older surveys. As this is so close to zero it was removed as a question from more recent surveys and we use the most recent value.
    CARERS_EMPLOYING_PERCENTAGE: float = 0.0063872289536592
    DIFFERENCE_IN_BASES_THRESHOLD: float = 100.0
    PROPORTION_EMPLOYING_STAFF_THRESHOLD: float = 0.1
    NUMBER_OF_YEARS_ROLLING_AVERAGE: int = 3
    FIRST_YEAR = 2011
