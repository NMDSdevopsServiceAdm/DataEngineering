import unittest

from schemas.spss_job_estimates_schema import SPSS_JOBS_ESTIMATES


class TestSpssJobEstimatesSchema(unittest.TestCase):
    def test_schema_structure_is_as_expected(self):
        estimates_schema = SPSS_JOBS_ESTIMATES
        expected_schema_structure = [
            {"name": "LOCATIONID", "type": "string", "nullable": True, "metadata": {}},
            {
                "name": "Main_Service_Group",
                "type": "integer",
                "nullable": False,
                "metadata": {},
            },
            {
                "name": "Care_homes_beds",
                "type": "float",
                "nullable": False,
                "metadata": {},
            },
            {
                "name": "WEIGHTING_CSSR",
                "type": "integer",
                "nullable": False,
                "metadata": {},
            },
            {
                "name": "WEIGHTING_REGION",
                "type": "integer",
                "nullable": False,
                "metadata": {},
            },
            {"name": "totalstaff", "type": "float", "nullable": False, "metadata": {}},
            {"name": "wkrrecs", "type": "float", "nullable": False, "metadata": {}},
            {"name": "jr28work", "type": "float", "nullable": False, "metadata": {}},
            {"name": "Data_Used", "type": "integer", "nullable": True, "metadata": {}},
            {"name": "All_jobs", "type": "float", "nullable": True, "metadata": {}},
            {
                "name": "Snapshot_date",
                "type": "string",
                "nullable": True,
                "metadata": {},
            },
            {
                "name": "Local_authority",
                "type": "string",
                "nullable": False,
                "metadata": {},
            },
            {"name": "Region", "type": "string", "nullable": False, "metadata": {}},
            {
                "name": "Main_service",
                "type": "string",
                "nullable": False,
                "metadata": {},
            },
            {
                "name": "Data_used_string",
                "type": "string",
                "nullable": False,
                "metadata": {},
            },
        ]

        actual_schema = estimates_schema.jsonValue().get("fields")
        self.assertEqual(actual_schema, expected_schema_structure)
