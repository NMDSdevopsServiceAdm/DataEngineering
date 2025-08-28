from dataclasses import dataclass


@dataclass
class ManualPostcodeCorrections:
    """
    Contains a mapping of known invalid or misrecorded postcodes to their corrected forms.
    The dictionary keys are the invalid postcodes.
    The values are the corrected replacements.
    """

    postcode_corrections_dict = {
        "CH52LY": "CH16HU",  # Welsh postcode, replaced with nearest English postcode
        "HU17ORH": "HU170RH",  # Replaced letter 'O' with zero '0'
        "TF73QH": "TF74EH",  # Incorrectly entered and no other postcodes exist starting 'TF7 3'
    }
