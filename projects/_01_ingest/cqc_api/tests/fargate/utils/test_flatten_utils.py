import unittest

import polars as pl
import polars.testing as pl_testing

from projects._01_ingest.cqc_api.fargate.utils import flatten_utils as job
from projects._01_ingest.unittest_data.polars_ingest_test_file_data import (
    FlattenUtilsData as Data,
)
from projects._01_ingest.unittest_data.polars_ingest_test_file_schema import (
    FlattenUtilsSchema as Schemas,
)
from utils.column_names.cleaned_data_files.cqc_location_cleaned import (
    CqcLocationCleanedColumns as CQCLClean,
)

PATCH_PATH = "projects._01_ingest.cqc_api.fargate.utils.flatten_utils"
