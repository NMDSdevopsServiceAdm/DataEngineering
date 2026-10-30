from typing import Any

import polars as pl
import polars.testing as pl_testing
import pytest

import projects._03_independent_cqc._02_employment_status.fargate.utils.model_utils as job
from polars_utils.column_types import CategoricalColumnTypes as CatColType
from projects._03_independent_cqc._02_employment_status.unittest_data.polars_employment_status_model_test_data import (
    IMPUTED_SHARE,
    KNOWN_SHARE,
    ROLLING_AVERAGE_SHARE,
)
from projects._03_independent_cqc._02_employment_status.unittest_data.polars_employment_status_model_test_data import (
    TestModelUtilsData as Data,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.ind_cqc_pipeline_columns import (
    ShareModelColumns as ShareModel,
)
from utils.column_values.categorical_column_values import ImputationRowKind

LOCATION_ROLE_COLUMNS = [IndCQC.location_id, IndCQC.published_job_role_label]
LOCATION_ROLE_DATE_COLUMNS = [*LOCATION_ROLE_COLUMNS, IndCQC.cqc_location_import_date]
AS_LOCATION_TYPE = pl.col(IndCQC.location_id).cast(CatColType.LocationCatType)

SCHEMA_OVERRIDES = {
    KNOWN_SHARE: pl.Float32,
    IMPUTED_SHARE: pl.Float32,
    ROLLING_AVERAGE_SHARE: pl.Float32,
    ShareModel.imputation_row_kind: pl.Enum(
        [
            ImputationRowKind.known,
            ImputationRowKind.interpolated,
            ImputationRowKind.carried,
        ]
    ),
}


def to_lf(data: dict[str, Any]) -> pl.LazyFrame:
    """Build a LazyFrame with the column types the functions use."""
    return pl.LazyFrame(
        data,
        schema_overrides={
            col: dtype for col, dtype in SCHEMA_OVERRIDES.items() if col in data
        },
    )


class TestAddImputationRowKind:
    @pytest.mark.parametrize(
        "case",
        [
            pytest.param(case, id=case.id)
            for case in Data.add_imputation_row_kind_test_cases
        ],
    )
    def test_row_kind_labels_known_interpolated_and_carried(self, case):
        returned_lf = job.add_imputation_row_kind(
            to_lf(case.input_data),
            known_column=KNOWN_SHARE,
            imputed_column=IMPUTED_SHARE,
            partition_columns=LOCATION_ROLE_COLUMNS,
            date_column=IndCQC.cqc_location_import_date,
        )

        pl_testing.assert_frame_equal(returned_lf, to_lf(case.expected_data))


class TestBuildModellingDataset:
    def test_builder_does_not_duplicate_rows(self):
        # Location IDs are categorical in both datasets, as in the pipeline.
        shares_lf = to_lf(Data.build_modelling_dataset_shares_data).with_columns(
            AS_LOCATION_TYPE
        )
        estimates_lf = pl.LazyFrame(
            Data.build_modelling_dataset_estimates_data
        ).with_columns(AS_LOCATION_TYPE)

        returned_lf = job.build_modelling_dataset(
            shares_lf,
            estimates_lf,
            known_share_columns=[KNOWN_SHARE],
            imputed_share_columns=[IMPUTED_SHARE],
            rolling_average_columns=[ROLLING_AVERAGE_SHARE],
        )

        pl_testing.assert_frame_equal(
            returned_lf.select(LOCATION_ROLE_DATE_COLUMNS),
            shares_lf.select(LOCATION_ROLE_DATE_COLUMNS),
            check_row_order=False,
        )
