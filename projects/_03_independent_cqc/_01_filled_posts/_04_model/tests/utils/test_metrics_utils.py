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
    METRICS_CARE_HOME_BEDS,
    cross_validation_features_data,
    metrics_care_home_features_data,
    metrics_care_home_locations_data,
    metrics_locations_data,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.ind_cqc_pipeline_columns import (
    ModelEvaluationColumns as ModelEvaluation,
)
from utils.column_names.ind_cqc_pipeline_columns import (
    ModelEvaluationLabels as Labels,
)
from utils.column_names.ind_cqc_pipeline_columns import ModelRegistryKeys as MRKeys

MODEL = "test_model"
LEVELS = {
    Labels.coverage,
    Labels.headline_group_totals,
    Labels.headline_by_year,
    Labels.period_totals,
    Labels.size_band,
    Labels.row_level_posts,
    Labels.row_level_metadata_scale,
    Labels.imputed_target_rows,
    Labels.imputed_target_period_bias,
}
FEATURES = [IndCQC.activity_count_capped, IndCQC.service_count_capped]
CARE_HOME_SPEC = {
    **Data.spec,
    MRKeys.dependent: IndCQC.imputed_filled_posts_per_bed_ratio_model,
}


def folds_lf() -> pl.LazyFrame:
    return pl.LazyFrame(
        Data.folds_data, schema_overrides={ModelEvaluation.fold: pl.UInt8}
    )


def oof_df() -> pl.DataFrame:
    return cv.predict_out_of_fold(
        pl.LazyFrame(cross_validation_features_data()), folds_lf(), Data.spec
    )


def care_home_oof_df() -> pl.DataFrame:
    return cv.predict_out_of_fold(
        pl.LazyFrame(metrics_care_home_features_data()), folds_lf(), CARE_HOME_SPEC
    )


def value(
    df: pl.DataFrame, level: str, metric: str, fold: str = Labels.pooled
) -> float:
    return df.filter(
        (pl.col(ModelEvaluation.level) == level)
        & (pl.col(ModelEvaluation.metric) == metric)
        & (pl.col(ModelEvaluation.fold) == fold)
    )[ModelEvaluation.value][0]


class TestToLong:
    def test_returns_a_row_per_metric_value(self):
        scores_df = pl.DataFrame({"r2": [0.9], "rmse": [2.0]})

        returned_df = job.to_long(scores_df, MODEL, "level")

        assert returned_df.columns == [
            ModelEvaluation.model,
            ModelEvaluation.level,
            ModelEvaluation.group,
            ModelEvaluation.fold,
            ModelEvaluation.metric,
            ModelEvaluation.value,
        ]
        assert returned_df[ModelEvaluation.metric].to_list() == ["r2", "rmse"]
        assert returned_df[ModelEvaluation.value].to_list() == [0.9, 2.0]

    def test_group_is_all_and_fold_is_pooled_when_neither_given(self):
        returned_df = job.to_long(pl.DataFrame({"r2": [0.9]}), MODEL, "level")

        assert returned_df[ModelEvaluation.group].to_list() == [Labels.all_groups]
        assert returned_df[ModelEvaluation.fold].to_list() == [Labels.pooled]

    def test_joins_group_columns_and_takes_fold_from_its_column(self):
        scores_df = pl.DataFrame({"a": ["x"], "b": [2], "fold": [1], "r2": [0.9]})

        returned_df = job.to_long(scores_df, MODEL, "level", ["a", "b"], "fold")

        assert returned_df[ModelEvaluation.group].to_list() == ["x | 2"]
        assert returned_df[ModelEvaluation.fold].to_list() == ["1"]
        assert returned_df[ModelEvaluation.metric].to_list() == ["r2"]


class TestScoreOutOfFold:
    @pytest.fixture
    def scores_df(self) -> pl.DataFrame:
        return job.score_out_of_fold(
            MODEL, Data.spec, oof_df(), pl.LazyFrame(metrics_locations_data())
        )

    def test_returns_every_level(self, scores_df):
        assert set(scores_df[ModelEvaluation.level].unique()) == LEVELS

    def test_perfect_predictions_have_no_headline_error(self, scores_df):
        error = value(
            scores_df,
            Labels.headline_group_totals,
            ModelEvaluation.weighted_absolute_percentage_error,
        )

        assert error == pytest.approx(0, abs=1e-4)

    def test_scores_every_fold_and_pooled(self, scores_df):
        folds = scores_df.filter(
            pl.col(ModelEvaluation.level) == Labels.row_level_posts
        )[ModelEvaluation.fold].unique()

        assert set(folds) == {"0", "1", "2", Labels.pooled}

    def test_counts_headline_groups(self, scores_df):
        number_of_groups = value(
            scores_df, Labels.headline_group_totals, ModelEvaluation.number_of_groups
        )

        assert number_of_groups == 1

    def test_known_levels_skip_rows_without_a_known_value(self, scores_df):
        known_rows = value(
            scores_df, Labels.row_level_posts, ModelEvaluation.number_of_rows
        )

        assert known_rows == 15

    def test_imputed_target_levels_score_rows_without_a_known_value(self, scores_df):
        target_rows = value(
            scores_df, Labels.imputed_target_rows, ModelEvaluation.number_of_rows
        )

        assert target_rows == 18

    def test_clips_predictions_at_one_except_on_the_metadata_scale(self):
        below_one_df = oof_df().with_columns(pl.lit(-100.0).alias(IndCQC.prediction))
        locations_df = pl.DataFrame(metrics_locations_data())
        known = locations_df[IndCQC.ascwds_filled_posts_dedup_clean].drop_nulls()

        scores_df = job.score_out_of_fold(
            MODEL, Data.spec, below_one_df, locations_df.lazy()
        )

        expected_bias = (known.len() - known.sum()) / known.sum()
        assert value(
            scores_df, Labels.row_level_posts, ModelEvaluation.bias
        ) == pytest.approx(expected_bias)
        assert value(scores_df, Labels.row_level_posts, IndCQC.rmse) < 100
        assert value(scores_df, Labels.row_level_metadata_scale, IndCQC.rmse) > 100

    def test_coverage_is_scored_known_rows_over_known_rows(self):
        oof_without_loc0_df = oof_df().filter(pl.col(IndCQC.location_id) != "loc0")

        scores_df = job.score_out_of_fold(
            MODEL,
            Data.spec,
            oof_without_loc0_df,
            pl.LazyFrame(metrics_locations_data()),
        )

        assert value(scores_df, Labels.coverage, ModelEvaluation.coverage) == (
            pytest.approx(12 / 15)
        )


class TestScoreOutOfFoldForCareHomes:
    """Care home models predict a per bed ratio, which is scored in filled posts."""

    LOCATIONS_LF = pl.LazyFrame(metrics_care_home_locations_data())
    POSTS_OFFSET = 20.0

    @pytest.fixture
    def offset_scores_df(self) -> pl.DataFrame:
        offset_oof_df = care_home_oof_df().with_columns(
            pl.col(IndCQC.prediction) + self.POSTS_OFFSET / METRICS_CARE_HOME_BEDS
        )
        return job.score_out_of_fold(
            "any_other_name", CARE_HOME_SPEC, offset_oof_df, self.LOCATIONS_LF
        )

    def test_returns_every_level(self):
        scores_df = job.score_out_of_fold(
            IndCQC.care_home_model,
            CARE_HOME_SPEC,
            care_home_oof_df(),
            self.LOCATIONS_LF,
        )

        assert set(scores_df[ModelEvaluation.level].unique()) == LEVELS

    def test_converts_ratio_predictions_to_filled_posts(self, offset_scores_df):
        known = (
            self.LOCATIONS_LF.select(
                pl.col(IndCQC.ascwds_filled_posts_dedup_clean).drop_nulls()
            )
            .collect()
            .to_series()
        )

        bias = value(offset_scores_df, Labels.row_level_posts, ModelEvaluation.bias)

        assert bias == pytest.approx(self.POSTS_OFFSET * known.len() / known.sum())

    def test_scores_the_metadata_scale_in_filled_posts_whatever_the_model_name(
        self, offset_scores_df
    ):
        within_ten = value(
            offset_scores_df,
            Labels.row_level_metadata_scale,
            IndCQC.proportion_of_model_predictions_within_ten,
        )
        within_twenty_five = value(
            offset_scores_df,
            Labels.row_level_metadata_scale,
            IndCQC.proportion_of_model_predictions_within_twenty_five,
        )

        assert within_ten == 0.0
        assert within_twenty_five == 1.0

    def test_size_bands_are_the_banded_beds(self, offset_scores_df):
        bands = offset_scores_df.filter(
            pl.col(ModelEvaluation.level) == Labels.size_band
        )[ModelEvaluation.group].unique()

        assert bands.to_list() == ["3"]

    def test_imputed_target_is_ratio_times_beds(self):
        scores_df = job.score_out_of_fold(
            IndCQC.care_home_model,
            CARE_HOME_SPEC,
            care_home_oof_df(),
            self.LOCATIONS_LF,
        )

        target_bias = value(scores_df, Labels.imputed_target_rows, ModelEvaluation.bias)

        assert target_bias == pytest.approx(0, abs=1e-4)


class TestDescribeFit:
    def fitted(self, **params):
        return cv.fit_model(
            pl.DataFrame(cross_validation_features_data()),
            {**Data.spec, "model_type": "lasso", "model_params": params},
        )

    def test_labels_scores_as_all_rows_not_pooled(self):
        model = self.fitted(alpha=0.0001)

        returned_df, _ = job.describe_fit(MODEL, model, FEATURES)

        assert set(returned_df[ModelEvaluation.fold]) == {Labels.all_rows}

    def test_flags_a_fit_that_stopped_at_max_iter(self):
        model = self.fitted(alpha=0.0001, max_iter=1)

        returned_df, _ = job.describe_fit(MODEL, model, FEATURES)

        assert (
            value(
                returned_df,
                Labels.fit_diagnostics,
                ModelEvaluation.hit_max_iter,
                Labels.all_rows,
            )
            == 1.0
        )

    def test_counts_exactly_zero_coefficients(self):
        model = self.fitted(alpha=1_000_000)

        returned_df, _ = job.describe_fit(MODEL, model, FEATURES)

        assert (
            value(
                returned_df,
                Labels.fit_diagnostics,
                ModelEvaluation.zero_coefficients,
                Labels.all_rows,
            )
            == 2
        )

    def test_returns_each_features_coefficient(self):
        model = self.fitted(alpha=0.0001)

        _, coefficients = job.describe_fit(MODEL, model, FEATURES)

        assert list(coefficients) == FEATURES
        assert all(c != 0 for c in coefficients.values())

    def test_linear_regression_does_not_hit_max_iter(self):
        model = cv.fit_model(pl.DataFrame(cross_validation_features_data()), Data.spec)

        returned_df, _ = job.describe_fit(MODEL, model, FEATURES)

        assert (
            value(
                returned_df,
                Labels.fit_diagnostics,
                ModelEvaluation.hit_max_iter,
                Labels.all_rows,
            )
            == 0.0
        )


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

        assert value(returned_df, Labels.jumpiness, ModelEvaluation.locations) == 2

    def test_counts_the_rows_sampled(self, features_lf):
        returned_df = self.score(features_lf)

        assert value(returned_df, Labels.jumpiness, ModelEvaluation.rows) == 6

    def test_caps_the_sample_size(self, features_lf):
        returned_df = self.score(features_lf, sample_size=1)

        assert value(returned_df, Labels.jumpiness, ModelEvaluation.locations) == 1

    def test_same_seed_gives_same_sample(self, features_lf):
        first_df = self.score(features_lf, seed=7, sample_size=1)
        second_df = self.score(features_lf, seed=7, sample_size=1)

        assert first_df.equals(second_df)
