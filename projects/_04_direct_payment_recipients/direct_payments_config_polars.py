from dataclasses import dataclass

from utils.column_values.categorical_column_values import ContemporaryCSSR


@dataclass
class DirectPaymentConfiguration:
    # The carer's employing percentage was calculated from a question in older surveys. As this is so close to zero it was removed as a question from more recent surveys and we use the most recent value.
    CARERS_EMPLOYING_PERCENTAGE: float = 0.0063872289536592
    PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF_THRESHOLD: float = 1.0
    ADASS_PROPORTION_OUTLIER_THRESHOLD: float = 0.3
    SELF_EMPLOYED_STAFF_PER_SERVICE_USER: float = 0.0179487641096729
    NUMBER_OF_YEARS_ROLLING_AVERAGE: int = 3
    FIRST_YEAR: int = 2011


@dataclass
class DirectPaymentsMisspelledLaNames:
    DICT_TO_CORRECT_LA_NAMES = {
        "Bath & N E Somerset": ContemporaryCSSR.bath_and_north_east_somerset,
        "Blackburn": ContemporaryCSSR.blackburn_with_darwen,
        "Bournemouth, Christchurch and Poole": ContemporaryCSSR.bournemouth_christchurch_and_poole,
        "East Riding": ContemporaryCSSR.east_riding_of_yorkshire,
        "Medway Towns": ContemporaryCSSR.medway,
        "Southend": ContemporaryCSSR.southend_on_sea,
    }
