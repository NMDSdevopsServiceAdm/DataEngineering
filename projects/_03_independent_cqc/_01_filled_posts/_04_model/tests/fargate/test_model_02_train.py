from contextlib import ExitStack
from datetime import date
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import polars as pl
import pytest

import projects._03_independent_cqc._01_filled_posts._04_model.fargate.model_02_train as job
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.ind_cqc_pipeline_columns import (
    ModelEvaluationColumns as ModelEvaluation,
)
from utils.column_names.ind_cqc_pipeline_columns import (
    ModelEvaluationLabels as Labels,
)
from utils.column_names.ind_cqc_pipeline_columns import ModelMetadataKeys as MMKeys
from utils.column_values.categorical_column_values import CareHome
from utils.column_names.ind_cqc_pipeline_columns import ModelRegistryKeys as MRKeys

PATCH_PATH = (
    "projects._03_independent_cqc._01_filled_posts._04_model.fargate.model_02_train"
)

BUCKET_NAME = "some_bucket"
MODEL_NAME = "my_model"
FEATURES = ["feat1", "feat2"]


def registry(auto_retrain: bool = True, dependent: str = "dependent_col") -> dict:
    return {
        MODEL_NAME: {
            MRKeys.version: "1.0.0",
            MRKeys.auto_retrain: auto_retrain,
            MRKeys.model_type: "lasso",
            MRKeys.model_params: {"alpha": 0.1},
            MRKeys.dependent: dependent,
            MRKeys.features: FEATURES,
        }
    }


def long_scores(level: str, fold: str, metric: str, value: float) -> pl.DataFrame:
    return pl.DataFrame(
        {
            ModelEvaluation.model: [MODEL_NAME],
            ModelEvaluation.level: [level],
            ModelEvaluation.group: [Labels.all_groups],
            ModelEvaluation.fold: [fold],
            ModelEvaluation.metric: [metric],
            ModelEvaluation.value: [value],
        }
    )


@pytest.fixture
def mocks():
    """Patches everything `main` reads from, computes with or writes to."""
    targets = {
        "scan_parquet": f"{PATCH_PATH}.utils.scan_parquet",
        "assign_location_folds": f"{PATCH_PATH}.evaluation.assign_location_folds",
        "predict_out_of_fold": f"{PATCH_PATH}.cvUtils.predict_out_of_fold",
        "fit_on_all_rows": f"{PATCH_PATH}.cvUtils.fit_on_all_rows",
        "score_out_of_fold": f"{PATCH_PATH}.metricsUtils.score_out_of_fold",
        "describe_fit": f"{PATCH_PATH}.metricsUtils.describe_fit",
        "score_jumpiness": f"{PATCH_PATH}.metricsUtils.score_jumpiness",
        "get_run_number": f"{PATCH_PATH}.vUtils.get_run_number",
        "save_metrics": f"{PATCH_PATH}.vUtils.save_metrics",
        "save_model_and_metadata": f"{PATCH_PATH}.vUtils.save_model_and_metadata",
    }
    with ExitStack() as stack:
        patched = SimpleNamespace(
            **{name: stack.enter_context(patch(t)) for name, t in targets.items()}
        )
        patched.get_run_number.return_value = 3
        patched.score_out_of_fold.return_value = pl.concat(
            [
                long_scores(
                    Labels.row_level_metadata_scale, Labels.pooled, IndCQC.r2, 0.8
                ),
                long_scores(Labels.row_level_metadata_scale, "0", IndCQC.r2, 0.1),
                long_scores(
                    Labels.coverage, Labels.pooled, ModelEvaluation.coverage, 1.0
                ),
            ]
        )
        patched.describe_fit.return_value = (
            long_scores(
                Labels.fit_diagnostics, Labels.all_rows, ModelEvaluation.n_iter, 5.0
            ),
            {"feat1": 0.5},
        )
        patched.score_jumpiness.return_value = long_scores(
            Labels.jumpiness,
            Labels.pooled,
            ModelEvaluation.mean_period_to_period_change,
            0.02,
        )
        stack.enter_context(patch(f"{PATCH_PATH}.model_registry", registry()))
        stack.enter_context(patch(f"{PATCH_PATH}.validate_model_definition"))
        yield patched


class TestMain:
    def test_skips_everything_when_auto_retrain_is_false(self, mocks):
        with patch(f"{PATCH_PATH}.model_registry", registry(auto_retrain=False)):
            job.main(BUCKET_NAME, MODEL_NAME)

        mocks.scan_parquet.assert_not_called()
        mocks.predict_out_of_fold.assert_not_called()
        mocks.fit_on_all_rows.assert_not_called()
        mocks.save_metrics.assert_not_called()
        mocks.save_model_and_metadata.assert_not_called()

    def test_saves_metrics_and_model_under_the_next_run_number(self, mocks):
        job.main(BUCKET_NAME, MODEL_NAME)

        assert mocks.save_metrics.call_args.args[1] == 4
        assert mocks.save_model_and_metadata.call_args.args[1] == 4

    def test_saves_metrics_before_the_model_so_partial_runs_are_not_found(self, mocks):
        calls = MagicMock()
        calls.attach_mock(mocks.save_metrics, "save_metrics")
        calls.attach_mock(mocks.save_model_and_metadata, "save_model_and_metadata")

        job.main(BUCKET_NAME, MODEL_NAME)

        saved = [call[0] for call in calls.mock_calls]
        assert saved == ["save_metrics", "save_model_and_metadata"]

    def test_saves_the_model_fitted_on_all_rows(self, mocks):
        job.main(BUCKET_NAME, MODEL_NAME)

        saved_model = mocks.save_model_and_metadata.call_args.args[2]
        assert saved_model is mocks.fit_on_all_rows.return_value

    def test_saves_every_set_of_scores_and_the_coefficients(self, mocks):
        job.main(BUCKET_NAME, MODEL_NAME)

        saved_metrics_df, saved_coefficients = mocks.save_metrics.call_args.args[2:]
        assert set(saved_metrics_df[ModelEvaluation.level]) == {
            Labels.row_level_metadata_scale,
            Labels.coverage,
            Labels.fit_diagnostics,
            Labels.jumpiness,
        }
        assert saved_coefficients == {"feat1": 0.5}

    def test_metadata_metrics_are_the_pooled_metadata_scale_scores(self, mocks):
        job.main(BUCKET_NAME, MODEL_NAME)

        saved_metadata = mocks.save_model_and_metadata.call_args.args[3]
        assert saved_metadata["metrics"] == {IndCQC.r2: 0.8}

    def test_saves_registry_features_in_metadata(self, mocks):
        job.main(BUCKET_NAME, MODEL_NAME)

        saved_metadata = mocks.save_model_and_metadata.call_args.args[3]
        assert saved_metadata[MMKeys.feature_columns] == FEATURES

    def test_predicts_out_of_fold_with_the_registry_entry(self, mocks):
        job.main(BUCKET_NAME, MODEL_NAME)

        spec = mocks.predict_out_of_fold.call_args.args[2]
        assert spec == registry()[MODEL_NAME]


class TestBuildLocationsLf:
    """
    Rows of one care home type, scored against ASC-WDS only (care homes) or ASC-WDS plus PIR
    (non-res).
    """

    DATE = date(2025, 1, 1)
    # location, care home, ASC-WDS value, ASC-WDS plus PIR value
    IMPUTED_ROWS = [
        ("ch_asc", CareHome.care_home, 10.0, 10.0),
        ("ch_pir", CareHome.care_home, None, 5.0),
        ("nr_asc", CareHome.not_care_home, 20.0, 20.0),
        ("nr_pir", CareHome.not_care_home, None, 6.0),
        ("nr_none", CareHome.not_care_home, None, None),
    ]

    def build(self, is_ratio_model: bool) -> pl.DataFrame:
        imputed_lf = pl.LazyFrame(
            {
                IndCQC.location_id: [r[0] for r in self.IMPUTED_ROWS],
                IndCQC.cqc_location_import_date: [self.DATE] * len(self.IMPUTED_ROWS),
                IndCQC.care_home: [r[1] for r in self.IMPUTED_ROWS],
                IndCQC.primary_service_type: ["service"] * len(self.IMPUTED_ROWS),
                IndCQC.current_cssr: ["cssr"] * len(self.IMPUTED_ROWS),
                IndCQC.number_of_beds: [10] * len(self.IMPUTED_ROWS),
                IndCQC.number_of_beds_banded: [1] * len(self.IMPUTED_ROWS),
                IndCQC.ascwds_pir_merged: [r[3] for r in self.IMPUTED_ROWS],
                IndCQC.imputed_filled_post_model: [1.0] * len(self.IMPUTED_ROWS),
                IndCQC.imputed_filled_posts_per_bed_ratio_model: [0.1]
                * len(self.IMPUTED_ROWS),
            },
            schema_overrides={IndCQC.ascwds_pir_merged: pl.Float64},
        )
        cleaned_lf = pl.LazyFrame(
            {
                IndCQC.location_id: [r[0] for r in self.IMPUTED_ROWS],
                IndCQC.cqc_location_import_date: [self.DATE] * len(self.IMPUTED_ROWS),
                IndCQC.ascwds_filled_posts_dedup_clean: [
                    r[2] for r in self.IMPUTED_ROWS
                ],
            },
            schema_overrides={IndCQC.ascwds_filled_posts_dedup_clean: pl.Float64},
        )

        return job.build_locations_lf(imputed_lf, cleaned_lf, is_ratio_model).collect()

    def known_by_location(self, df: pl.DataFrame) -> dict:
        return dict(
            zip(
                df[IndCQC.location_id],
                df[IndCQC.ascwds_filled_posts_dedup_clean],
            )
        )

    def test_non_res_known_values_include_pir(self):
        known = self.known_by_location(self.build(is_ratio_model=False))

        assert known == {"nr_asc": 20.0, "nr_pir": 6.0, "nr_none": None}

    def test_care_home_known_values_exclude_pir(self):
        known = self.known_by_location(self.build(is_ratio_model=True))

        assert known == {"ch_asc": 10.0, "ch_pir": None}

    def test_non_res_keeps_only_non_res_locations(self):
        returned_df = self.build(is_ratio_model=False)

        assert set(returned_df[IndCQC.location_id]) == {"nr_asc", "nr_pir", "nr_none"}

    def test_care_home_keeps_only_care_home_locations(self):
        returned_df = self.build(is_ratio_model=True)

        assert set(returned_df[IndCQC.location_id]) == {"ch_asc", "ch_pir"}
