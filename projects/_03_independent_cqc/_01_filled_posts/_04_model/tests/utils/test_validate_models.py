import polars as pl
import pytest

import projects._03_independent_cqc._01_filled_posts._04_model.utils.validate_models as job
from projects._03_independent_cqc._01_filled_posts.unittest_data.polars_ind_cqc_test_file_data import (
    ValidateModelsData as Data,
)
from projects._03_independent_cqc._01_filled_posts.unittest_data.polars_ind_cqc_test_file_schemas import (
    ValidateModelsSchemas as Schemas,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC


class TestGetExpectedRowCountForModelFeatures:
    def test_raises_error_for_unrecognised_model(self):
        test_lf = pl.LazyFrame(
            Data.validate_model_feature_rows,
            Schemas.validate_model_feature_schema,
            orient="row",
        )

        with pytest.raises(ValueError):
            job.get_expected_row_count_for_model_features(test_lf, "unrecognised_model")

    @pytest.mark.parametrize(
        "model",
        [
            IndCQC.non_res_with_dormancy_model,
            IndCQC.non_res_without_dormancy_model,
            IndCQC.care_home_model,
        ],
    )
    def test_counts_only_rows_passing_model_filters(self, model: str):
        test_lf = pl.LazyFrame(
            Data.validate_model_feature_rows,
            Schemas.validate_model_feature_schema,
            orient="row",
        )

        returned_row_count = job.get_expected_row_count_for_model_features(
            test_lf, model
        )

        assert returned_row_count == Data.expected_get_expected_row_count_rows

    def test_expected_row_count_ignores_unused_columns(self):
        test_lf = pl.LazyFrame(
            Data.null_posts_rolling_average_rows,
            Schemas.validate_model_feature_schema,
            orient="row",
        )

        returned_row_count = job.get_expected_row_count_for_model_features(
            test_lf, IndCQC.non_res_with_dormancy_model
        )

        assert returned_row_count == Data.expected_null_posts_rolling_average_row_count

    def test_care_home_count_excludes_rows_missing_bed_features(self):
        test_lf = pl.LazyFrame(
            Data.care_home_missing_bed_feature_rows,
            Schemas.validate_model_feature_schema,
            orient="row",
        )

        returned_row_count = job.get_expected_row_count_for_model_features(
            test_lf, IndCQC.care_home_model
        )

        assert (
            returned_row_count == Data.expected_care_home_missing_bed_feature_row_count
        )
