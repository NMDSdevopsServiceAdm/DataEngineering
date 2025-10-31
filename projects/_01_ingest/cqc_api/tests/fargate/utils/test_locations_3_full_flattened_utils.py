import unittest

import polars as pl
import polars.testing as pl_testing

from projects._01_ingest.cqc_api.fargate.utils import (
    locations_3_full_flattened_utils as job,
)
from projects._01_ingest.unittest_data.polars_ingest_test_file_data import (
    FullFlattenUtilsData as Data,
)
from projects._01_ingest.unittest_data.polars_ingest_test_file_schema import (
    FullFlattenUtilsSchema as Schemas,
)

PATCH_PATH = (
    "projects._01_ingest.cqc_api.fargate.utils.locations_3_full_flattened_utils"
)


class GetImportDatesToProcessTests(unittest.TestCase):
    pass


class LoadLatestSnapshotTests(unittest.TestCase):
    pass


class CreateFullSnapshotTests(unittest.TestCase):
    pass


class ApplyPartitionsTests(unittest.TestCase):
    pass
