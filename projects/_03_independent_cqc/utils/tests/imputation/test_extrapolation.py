import polars as pl
import polars.testing as pl_testing
import pytest

import projects._03_independent_cqc.utils.imputation.extrapolation as job
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_data import (
    ModelExtrapolation as Data,
)
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_schemas import (
    ModelExtrapolation as Schemas,
)
from utils.column_names.ind_cqc_pipeline_columns import (
    ExtrapolationColumns as ExtrapCol,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC

PATCH_PATH = "projects._03_independent_cqc.utils.utils.imputation.extrapolation"


class TestExtrapolationWhenNominal:
    @pytest.mark.parametrize(
        "extrapolation_when_nominal_data",
        [case.as_pytest_param() for case in Data.extrapolation_when_nominal_test_cases],
    )
    def test_returned_extrapolation_values_match_expected_when_nominal(
        self, extrapolation_when_nominal_data
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
        self, extrapolation_when_ratio_data
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


class TestBuildExtrapolationAggregates:
    def test_returns_correct_aggregates_per_location(self):
        input_lf = pl.LazyFrame(
            Data.extrapolation_aggregates_rows,
            Schemas.extrapolation_aggregates_schema,
            orient="row",
        )

        returned_lf = job.build_extrapolation_aggregates(
            input_lf, IndCQC.ascwds_pir_merged, IndCQC.posts_rolling_average_model
        )

        expected_lf = pl.LazyFrame(
            Data.expected_extrapolation_aggregates_rows,
            Schemas.expected_extrapolation_aggregates_schema,
            orient="row",
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf)


class TestGetPreviousValue:
    @pytest.mark.parametrize(
        "get_previous_value_data",
        [case.as_pytest_param() for case in Data.get_previous_value_test_cases],
    )
    def test_returned_previous_values_match_expected(self, get_previous_value_data):
        expected_lf = pl.LazyFrame(
            get_previous_value_data,
            Schemas.get_previous_value_schema,
            orient="row",
        )
        input_lf = expected_lf.drop(ExtrapCol.previous_value)
        returned_lf = input_lf.with_columns(
            job.get_previous_value(IndCQC.ascwds_pir_merged).alias(
                ExtrapCol.previous_value
            )
        )

        pl_testing.assert_frame_equal(
            returned_lf,
            expected_lf,
            check_row_order=False,
        )


class TestExtrapolationExpressions:
    MODEL = "model"
    OUTPUT = "output"

    def test_forward_ratio(self):
        lf = pl.LazyFrame(
            {
                ExtrapCol.previous_value: [10.0],
                ExtrapCol.previous_model: [20.0],
                self.MODEL: [40.0],
            }
        )

        expr = job.ExtrapolationExpressions(self.MODEL)

        result = lf.select(expr.forward_ratio.alias(self.OUTPUT)).collect()

        assert result[self.OUTPUT][0] == 20.0  # 10 * 40 / 20

    def test_backward_ratio(self):
        lf = pl.LazyFrame(
            {
                job.TEMP.first_value: [10.0],
                job.TEMP.first_model: [20.0],
                self.MODEL: [10.0],
            }
        )

        expr = job.ExtrapolationExpressions(self.MODEL)

        result = lf.select(expr.backward_ratio.alias(self.OUTPUT)).collect()

        assert result[self.OUTPUT][0] == 5.0

    def test_forward_nominal(self):
        lf = pl.LazyFrame(
            {
                job.TEMP.previous_value: [10.0],
                job.TEMP.previous_model: [20.0],
                self.MODEL: [30.0],
            }
        )

        expr = job.ExtrapolationExpressions(self.MODEL)

        result = lf.select(expr.forward_nominal.alias(self.OUTPUT)).collect()

        assert result[self.OUTPUT][0] == 20.0  # 10 + (30 - 20)

    def test_backward_nominal(self):
        lf = pl.LazyFrame(
            {
                job.TEMP.first_value: [10.0],
                job.TEMP.first_model: [20.0],
                self.MODEL: [5.0],
            }
        )

        expr = job.ExtrapolationExpressions(self.MODEL)

        result = lf.select(expr.backward_nominal.alias(self.OUTPUT)).collect()

        assert result[self.OUTPUT][0] == -5.0  # 10 - (20 - 5)
