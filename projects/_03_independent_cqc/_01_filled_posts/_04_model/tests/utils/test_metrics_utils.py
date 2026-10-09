import polars as pl
import pytest

from projects._03_independent_cqc._01_filled_posts._04_model.utils import (
    cross_validation_utils as cv,
)
from projects._03_independent_cqc._01_filled_posts._04_model.utils import (
    metrics_utils as job,
)
from projects._03_independent_cqc._01_filled_posts.unittest_data.polars_ind_cqc_test_file_data import (
    CrossValidationUtilsData as Data,
)
from projects._03_independent_cqc._01_filled_posts.unittest_data.polars_ind_cqc_test_file_data import (
    cross_validation_features_data,
    metrics_locations_data,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.ind_cqc_pipeline_columns import (
    ModelEvaluationColumns as ModelEvaluation,
)

MODEL = "test_model"
LEVELS = {
    "coverage",
    "headline_group_totals",
    "headline_by_year",
    "period_totals",
    "size_band",
    "row_level_posts",
    "row_level_metadata_scale",
    "imputed_target_rows",
    "imputed_target_period_bias",
}
FEATURES = [IndCQC.activity_count_capped, IndCQC.service_count_capped]


def folds_lf() -> pl.LazyFrame:
    return pl.LazyFrame(
        Data.folds_data, schema_overrides={ModelEvaluation.fold: pl.UInt8}
    )


def oof_df() -> pl.DataFrame:
    return cv.predict_out_of_fold(
        pl.LazyFrame(cross_validation_features_data()), folds_lf(), Data.spec
    )


def value(df: pl.DataFrame, level: str, metric: str, fold: str = "pooled") -> float:
    return df.filter(
        (pl.col("level") == level)
        & (pl.col("metric") == metric)
        & (pl.col("fold") == fold)
    )["value"][0]


class TestToLong:
    def test_returns_a_row_per_metric_value(self):
        scores_df = pl.DataFrame({"r2": [0.9], "rmse": [2.0]})

        returned_df = job.to_long(scores_df, MODEL, "level")

        assert returned_df.columns == [
            "model",
            "level",
            "group",
            "fold",
            "metric",
            "value",
        ]
        assert returned_df["metric"].to_list() == ["r2", "rmse"]
        assert returned_df["value"].to_list() == [0.9, 2.0]

    def test_group_is_all_and_fold_is_pooled_when_neither_given(self):
        returned_df = job.to_long(pl.DataFrame({"r2": [0.9]}), MODEL, "level")

        assert returned_df["group"].to_list() == ["all"]
        assert returned_df["fold"].to_list() == ["pooled"]

    def test_joins_group_columns_and_takes_fold_from_its_column(self):
        scores_df = pl.DataFrame({"a": ["x"], "b": [2], "fold": [1], "r2": [0.9]})

        returned_df = job.to_long(scores_df, MODEL, "level", ["a", "b"], "fold")

        assert returned_df["group"].to_list() == ["x | 2"]
        assert returned_df["fold"].to_list() == ["1"]
        assert returned_df["metric"].to_list() == ["r2"]


class TestScoreOutOfFold:
    @pytest.fixture
    def scores_df(self) -> pl.DataFrame:
        return job.score_out_of_fold(
            MODEL, Data.spec, oof_df(), pl.LazyFrame(metrics_locations_data())
        )

    def test_returns_every_level(self, scores_df):
        assert set(scores_df["level"].unique()) == LEVELS

    def test_perfect_predictions_have_no_headline_error(self, scores_df):
        error = value(
            scores_df, "headline_group_totals", "weighted_absolute_percentage_error"
        )

        assert error == pytest.approx(0, abs=1e-4)

    def test_scores_every_fold_and_pooled(self, scores_df):
        folds = scores_df.filter(pl.col("level") == "row_level_posts")["fold"].unique()

        assert set(folds) == {"0", "1", "2", "pooled"}

    def test_coverage_is_scored_known_rows_over_known_rows(self):
        oof_without_loc0_df = oof_df().filter(pl.col(IndCQC.location_id) != "loc0")

        scores_df = job.score_out_of_fold(
            MODEL,
            Data.spec,
            oof_without_loc0_df,
            pl.LazyFrame(metrics_locations_data()),
        )

        assert value(scores_df, "coverage", "coverage") == pytest.approx(12 / 15)


class TestDescribeFit:
    def fitted(self, **params):
        return cv.fit_model(
            pl.DataFrame(cross_validation_features_data()),
            {**Data.spec, "model_type": "lasso", "model_params": params},
        )

    def test_flags_a_fit_that_stopped_at_max_iter(self):
        model = self.fitted(alpha=0.0001, max_iter=1)

        returned_df, _ = job.describe_fit(MODEL, model, FEATURES)

        assert value(returned_df, "fit_diagnostics", "hit_max_iter") == 1.0

    def test_counts_exactly_zero_coefficients(self):
        model = self.fitted(alpha=1_000_000)

        returned_df, _ = job.describe_fit(MODEL, model, FEATURES)

        assert value(returned_df, "fit_diagnostics", "zero_coefficients") == 2

    def test_returns_each_features_coefficient(self):
        model = self.fitted(alpha=0.0001)

        _, coefficients = job.describe_fit(MODEL, model, FEATURES)

        assert list(coefficients) == FEATURES
        assert all(c != 0 for c in coefficients.values())

    def test_linear_regression_does_not_hit_max_iter(self):
        model = cv.fit_model(pl.DataFrame(cross_validation_features_data()), Data.spec)

        returned_df, _ = job.describe_fit(MODEL, model, FEATURES)

        assert value(returned_df, "fit_diagnostics", "hit_max_iter") == 0.0


class TestScoreJumpiness:
    @pytest.fixture
    def features_lf(self) -> pl.LazyFrame:
        """loc4 and loc5 have no target at any date."""
        data = cross_validation_features_data()
        data[IndCQC.imputed_filled_post_model] = [
            None if location in ("loc4", "loc5") else dependent
            for location, dependent in zip(
                data[IndCQC.location_id], data[IndCQC.imputed_filled_post_model]
            )
        ]
        return pl.LazyFrame(data)

    def score(self, features_lf, seed=1, sample_size=10) -> pl.DataFrame:
        model = cv.fit_on_all_rows(features_lf, Data.spec)
        return job.score_jumpiness(
            MODEL,
            Data.spec,
            model,
            features_lf,
            folds_lf(),
            pl.LazyFrame(metrics_locations_data()),
            seed,
            sample_size,
        )

    def test_samples_only_locations_with_no_target(self, features_lf):
        returned_df = self.score(features_lf)

        assert value(returned_df, "jumpiness", "locations") == 2

    def test_caps_the_sample_size(self, features_lf):
        returned_df = self.score(features_lf, sample_size=1)

        assert value(returned_df, "jumpiness", "locations") == 1

    def test_same_seed_gives_same_sample(self, features_lf):
        first_df = self.score(features_lf, seed=7, sample_size=1)
        second_df = self.score(features_lf, seed=7, sample_size=1)

        assert first_df.equals(second_df)
