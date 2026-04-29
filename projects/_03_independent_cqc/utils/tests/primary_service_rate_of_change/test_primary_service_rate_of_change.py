import polars as pl
import polars.testing as pl_testing
import pytest

import projects._03_independent_cqc.utils.primary_service_rate_of_change as job
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_data import (
    ModelRateOfChangeData as Data,
)
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_schemas import (
    ModelRateOfChangeSchemas as Schemas,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.ind_cqc_pipeline_columns import (
    PrimaryServiceRateOfChangeColumns as TempCol,
)
