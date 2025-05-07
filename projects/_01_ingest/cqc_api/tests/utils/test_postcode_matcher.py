import unittest
from unittest.mock import Mock, patch

import projects._01_ingest.cqc_api.utils.postcode_matcher as job
from tests.test_file_data import PostcodeMatcherData as Data
from tests.test_file_schemas import PostcodeMatcherSchema as Schemas
from utils import utils
from utils.column_names.cleaned_data_files.cqc_location_cleaned import (
    CqcLocationCleanedColumns as CQCLCleaned,
)


class PostcodeMatcherTests(unittest.TestCase):
    def setUp(self) -> None:
        self.spark = utils.get_spark()


class MainTests(PostcodeMatcherTests):
    def setUp(self) -> None:
        super().setUp()

        locations_df = self.spark.createDataFrame(
            Data.locations_where_all_match_rows,
            Schemas.locations_schema,
        )

        returned_df = job.run_postcode_matching(locations_df)
