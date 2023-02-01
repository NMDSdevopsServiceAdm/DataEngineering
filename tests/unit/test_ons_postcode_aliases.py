import unittest
from dataclasses import asdict

from utils.prepare_locations_utils.ons_postcode_aliases import OnsPostcodeDataAliases


class LocationsFeatureEngineeringTests(unittest.TestCase):
    def test_ons_postcode_alias_has_all_required_attributes(self):
        ons_postcode_alias = OnsPostcodeDataAliases()
        expected_result = {
            "ons_postcode": "ons_postcode",
            "region_alias": "ons_region",
            "nhs_england_region_alias": "nhs_england_region",
            "country_alias": "country",
            "lsoa_alias": "lsoa",
            "msoa_alias": "msoa",
            "ccg_alias": "clinical_commisioning_group",
            "rural_urban_indicator_alias": "rural_urban_indicator",
            "import_date_alias": "ons_import_date",
        }
        self.assertEqual(asdict(ons_postcode_alias), expected_result)
