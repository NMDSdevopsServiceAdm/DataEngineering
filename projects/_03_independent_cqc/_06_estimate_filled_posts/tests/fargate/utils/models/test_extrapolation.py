import polars as pl
import polars.testing as pl_testing
import pytest

import projects._03_independent_cqc._06_estimate_filled_posts.fargate.utils.models.extrapolation as job
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_data import (
    ModelExtrapolation as Data,
)
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_schemas import (
    ModelExtrapolation as Schemas,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC

PATCH_PATH = "projects._03_independent_cqc._06_estimate_filled_posts.fargate.utils.models.extrapolation"


class TestExtrapolationWhenNominal:
    @pytest.mark.parametrize(
        "extrapolation_when_nominal_data",
        [case.as_pytest_param() for case in Data.extrapolation_when_nominal_test_cases],
    )
    def test_returned_extrapolation_values_match_expected_when_nominal(
        self,
        extrapolation_when_nominal_data,
    ):
        expected_nominal_lf = pl.LazyFrame(
            extrapolation_when_nominal_data,
            Schemas.model_extrapolation_schema,
            orient="row",
        )
        input_lf = expected_nominal_lf.drop(
            IndCQC.extrapolation_forwards, IndCQC.extrapolation_model
        )
        returned_nominal_lf = job.model_extrapolation(
            input_lf,
            column_with_null_values=IndCQC.ascwds_pir_merged,
            model_to_extrapolate_from=IndCQC.posts_rolling_average_model,
            extrapolation_method="nominal",
        )

        pl_testing.assert_frame_equal(
            returned_nominal_lf,
            expected_nominal_lf,
            abs_tol=0.00001,
            check_row_order=False,
        )


class TestExtrapolationWhenRatio:
    @pytest.mark.parametrize(
        "extrapolation_when_ratio_data",
        [case.as_pytest_param() for case in Data.extrapolation_when_ratio_test_cases],
    )
    def test_returned_extrapolation_values_match_expected_when_ratio(
        self,
        extrapolation_when_ratio_data,
    ):
        expected_ratio_lf = pl.LazyFrame(
            extrapolation_when_ratio_data,
            Schemas.model_extrapolation_schema,
            orient="row",
        )
        input_lf = expected_ratio_lf.drop(
            IndCQC.extrapolation_forwards, IndCQC.extrapolation_model
        )
        returned_ratio_lf = job.model_extrapolation(
            input_lf,
            column_with_null_values=IndCQC.ascwds_pir_merged,
            model_to_extrapolate_from=IndCQC.posts_rolling_average_model,
            extrapolation_method="ratio",
        )

        pl_testing.assert_frame_equal(
            returned_ratio_lf,
            expected_ratio_lf,
            abs_tol=0.00001,
            check_row_order=False,
        )


class TestExtrapolationWhenInvalidMethod:
    def test_error_raised_for_invalid_extrapolation_method(self):
        input_lf = pl.LazyFrame(
            Data.expected_extrapolation_when_error_rows,
            Schemas.model_extrapolation_schema,
            orient="row",
        ).drop(IndCQC.extrapolation_forwards, IndCQC.extrapolation_model)
        expected_error_message = "Error: method must be either 'ratio' or 'nominal'."
        with pytest.raises(ValueError, match=expected_error_message):
            job.model_extrapolation(
                input_lf,
                column_with_null_values=IndCQC.ascwds_pir_merged,
                model_to_extrapolate_from=IndCQC.posts_rolling_average_model,
                extrapolation_method="other",  # Invalid method
            )
