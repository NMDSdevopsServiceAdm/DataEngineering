from contextlib import ExitStack
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
