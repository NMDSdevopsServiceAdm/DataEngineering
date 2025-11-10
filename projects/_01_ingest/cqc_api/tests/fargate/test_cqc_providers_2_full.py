import unittest
from unittest.mock import Mock, patch

import projects._01_ingest.cqc_api.fargate.cqc_providers_2_full as job

PATCH_PATH = "projects._01_ingest.cqc_api.fargate.cqc_providers_2_full"


class CqcProvidersFullTests(unittest.TestCase):
    TEST_SOURCE = "some/source"
    TEST_DESTINATION = "some/destination"

    @patch(f"{PATCH_PATH}.convert_delta_to_full")
    def test_main_runs_successfully(self, convert_delta_to_full: Mock):

        job.main(self.TEST_SOURCE, self.TEST_DESTINATION)

        convert_delta_to_full.assert_called_once_with(
            self.TEST_SOURCE, self.TEST_DESTINATION, "providers"
        )
