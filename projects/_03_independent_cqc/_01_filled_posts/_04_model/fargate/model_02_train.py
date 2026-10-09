import polars as pl

from polars_utils import utils
from polars_utils.expressions import is_care_home, is_not_care_home
from projects._03_independent_cqc._01_filled_posts._04_model.registry.model_registry import (
    model_registry,
)
from projects._03_independent_cqc._01_filled_posts._04_model.utils import (
    cross_validation_utils as cvUtils,
)
from projects._03_independent_cqc._01_filled_posts._04_model.utils import (
    metrics_utils as metricsUtils,
)
from projects._03_independent_cqc._01_filled_posts._04_model.utils import paths
from projects._03_independent_cqc._01_filled_posts._04_model.utils import (
    versioning as vUtils,
)
from projects._03_independent_cqc._01_filled_posts._04_model.utils.validate_model_definitions import (
    validate_model_definition,
)
from projects._03_independent_cqc.utils import model_evaluation_utils as evaluation
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.ind_cqc_pipeline_columns import (
    ModelEvaluationColumns as ModelEvaluation,
)
from utils.column_names.ind_cqc_pipeline_columns import (
    ModelEvaluationLabels as Labels,
)
from utils.column_names.ind_cqc_pipeline_columns import ModelMetadataKeys as MMKeys
from utils.column_names.ind_cqc_pipeline_columns import ModelRegistryKeys as MRKeys


def main(bucket_name: str, model_name: str) -> None:
    """
    Scores a model on held-out locations, fits it on all training rows, and saves both.

    The steps in this function are:
        1. Validate the model definition and skip models with auto-retraining disabled
        2. Assign each location to a cross-validation fold
        3. Score out-of-fold predictions against known and imputed filled posts
        4. Fit the model on all training rows
        5. Describe the fit and score the jumpiness of its predictions
        6. Save the model, metadata, metrics and coefficients under a new run number

    Note: scikit-learn needs in-memory data, so features are collected for the fold
    predictions, the final fit and the jumpiness sample. The cleaned and imputed datasets are
    only read for the columns scoring needs.

    Args:
        bucket_name (str): the bucket (name only) in which to source the datasets from
        model_name (str): the name of the model to train
    """
    print(f"Training {model_name}...")

    n_folds = 5
    seed = 42
    jumpiness_sample_size = 10_000

    validate_model_definition(
        model_name,
        required_keys=[
            MRKeys.version,
            MRKeys.auto_retrain,
            MRKeys.model_type,
            MRKeys.model_params,
            MRKeys.dependent,
            MRKeys.features,
        ],
        model_registry=model_registry,
    )

    model_def = model_registry[model_name]

    if not model_def[MRKeys.auto_retrain]:
        print(f"Auto-retraining is disabled for {model_name}. Skipping training.")
        return

    features_lf = utils.scan_parquet(
        paths.generate_features_path(bucket_name, model_name)
    )

    is_ratio_model = (
        model_def[MRKeys.dependent] == IndCQC.imputed_filled_posts_per_bed_ratio_model
    )
    keys = [IndCQC.location_id, IndCQC.cqc_location_import_date]
    known_lf = utils.scan_parquet(paths.generate_cleaned_path(bucket_name)).select(
        *keys, IndCQC.ascwds_filled_posts_dedup_clean
    )
    locations_lf = (
        utils.scan_parquet(paths.generate_ind_cqc_path(bucket_name))
        .filter(is_care_home() if is_ratio_model else is_not_care_home())
        .select(
            *keys,
            IndCQC.primary_service_type,
            IndCQC.current_cssr,
            IndCQC.number_of_beds,
            IndCQC.number_of_beds_banded,
            IndCQC.imputed_filled_post_model,
            IndCQC.imputed_filled_posts_per_bed_ratio_model,
        )
        .join(known_lf, on=keys, how="left", validate="m:1")
    )

    folds_lf = (
        evaluation.assign_location_folds(
            features_lf.select(IndCQC.location_id).unique(),
            IndCQC.location_id,
            n_folds,
            seed,
        )
        .collect()
        .lazy()
    )

    oof_df = cvUtils.predict_out_of_fold(features_lf, folds_lf, model_def)
    scores_df = metricsUtils.score_out_of_fold(
        model_name, model_def, oof_df, locations_lf
    )

    model = cvUtils.fit_on_all_rows(features_lf, model_def)
    fit_df, coefficients = metricsUtils.describe_fit(
        model_name, model, model_def[MRKeys.features]
    )
    jumpiness_df = metricsUtils.score_jumpiness(
        model_name,
        model_def,
        model,
        features_lf,
        folds_lf,
        locations_lf,
        seed,
        jumpiness_sample_size,
    )

    metrics_df = pl.concat([scores_df, fit_df, jumpiness_df])
    pooled_metadata_scale_df = metrics_df.filter(
        (pl.col(ModelEvaluation.level) == Labels.row_level_metadata_scale)
        & (pl.col(ModelEvaluation.fold) == Labels.pooled)
    )

    assert pooled_metadata_scale_df.height, "no pooled metadata-scale metrics to save"

    metadata = {
        "name": model_name,
        "type": model_def[MRKeys.model_type],
        "parameters": model_def[MRKeys.model_params],
        "version": model_def[MRKeys.version],
        MMKeys.feature_columns: model_def[MRKeys.features],
        "dependent_column": model_def[MRKeys.dependent],
        "metrics": dict(
            zip(
                pooled_metadata_scale_df[ModelEvaluation.metric].to_list(),
                pooled_metadata_scale_df[ModelEvaluation.value].to_list(),
            )
        ),
    }

    model_path = paths.generate_model_path(
        bucket_name, model_name, model_def[MRKeys.version]
    )
    new_run_number = vUtils.get_run_number(model_path) + 1
    vUtils.save_metrics(model_path, new_run_number, metrics_df, coefficients)
    vUtils.save_model_and_metadata(model_path, new_run_number, model, metadata)

    print(f"{model_name} trained and saved with run number {new_run_number}.")


if __name__ == "__main__":

    args = utils.get_args(
        ("--bucket_name", "The bucket to source and save the datasets to"),
        ("--model_name", "The name of the model to create features for"),
    )

    main(bucket_name=args.bucket_name, model_name=args.model_name)
