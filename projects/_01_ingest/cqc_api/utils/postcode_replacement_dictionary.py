from dataclasses import dataclass


@dataclass
class ManualPostcodeCorrections:
    """
    Contains a mapping of known invalid or misrecorded postcodes to their corrected forms.
    The dictionary keys are the invalid postcodes.
    The values are the corrected replacements.
    """

    postcode_corrections_dict = {
        "CF105AS": None,  # Welsh postcode
        "CH52LY": "CH16HU",  # Welsh postcode, replaced with nearest English postcode
        "CH53DJ": "CH48PH",  # Welsh postcode, replaced with nearest English postcode
        "CH71AP": "CH48PH",  # Welsh postcode, replaced with nearest English postcode
        "DH66QZ": "DH96QZ",  # Incorrectly entered and no other postcodes exist starting 'DH6 6'
        "HU17ORH": "HU170RH",  # Replaced letter 'O' with zero '0'
        "LL137YP": "CH36QD",  # Welsh postcode, replaced with nearest English postcode
        "NP206QS": None,  # Welsh postcode
        "TF73QH": "TF74EH",  # Incorrectly entered and no other postcodes exist starting 'TF7 3'
        "W19RR": "W1G9RR",  # Incorrectly entered - missing 'G'
    }
