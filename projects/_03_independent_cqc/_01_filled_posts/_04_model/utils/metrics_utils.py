import numpy as np
import polars as pl
from sklearn.linear_model import LinearRegression
from sklearn.pipeline import Pipeline

from projects._03_independent_cqc._01_filled_posts._04_model.utils import (
    cross_validation_utils as cv,
)
from projects._03_independent_cqc._01_filled_posts._04_model.utils import model_utils
from projects._03_independent_cqc.utils import model_evaluation_utils as evaluation
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.ind_cqc_pipeline_columns import (
    ModelEvaluationColumns as ModelEvaluation,
)
from utils.column_names.ind_cqc_pipeline_columns import (
    ModelEvaluationLabels as Labels,
)
from utils.column_names.ind_cqc_pipeline_columns import ModelRegistryKeys as MRKeys

KEYS = [IndCQC.location_id, IndCQC.cqc_location_import_date]
KNOWN = IndCQC.ascwds_filled_posts_dedup_clean
PRED = IndCQC.prediction
FOLD = ModelEvaluation.fold
BAND = ModelEvaluation.size_band
TARGET_POSTS = ModelEvaluation.imputed_target_posts
RATIO_PRED = ModelEvaluation.ratio_prediction
UNCLIPPED_PRED = ModelEvaluation.prediction_unclipped
LOG_PRED = ModelEvaluation.log_prediction


def to_long(
    scores_df: pl.DataFrame,
    model_name: str,
    level: str,
    group_columns: list[str] | None = None,
    fold_column: str | None = None,
) -> pl.DataFrame:
    """
    Reshape scores to one row per metric value.

    Args:
        scores_df (pl.DataFrame): a row per group (and fold), with a column per metric
        model_name (str): the model the scores are for
        level (str): the kind of score, such as "size_band"
        group_columns (list[str] | None): columns joined with " | " into the group, which is
            "all" when there are none
        fold_column (str | None): the fold column. The fold is "pooled" when there is none.

    Returns:
        pl.DataFrame: "model", "level", "group", "fold", "metric" and "value" (Float64)
    """
    group_columns = group_columns or []
    id_columns = [c for c in (*group_columns, fold_column) if c]
    metrics = [c for c in scores_df.columns if c not in id_columns]
    group = (
        pl.concat_str(
            [pl.col(c).cast(pl.String) for c in group_columns], separator=" | "
        )
        if group_columns
        else pl.lit(Labels.all_groups)
    )
    fold = pl.col(fold_column).cast(pl.String) if fold_column else pl.lit(Labels.pooled)
    index = [ModelEvaluation.group, FOLD]

    return (
        scores_df.with_columns(group.alias(ModelEvaluation.group), fold.alias(FOLD))
        .select(*index, *[pl.col(m).cast(pl.Float64) for m in metrics])
        .unpivot(
            index=index,
            variable_name=ModelEvaluation.metric,
            value_name=ModelEvaluation.value,
        )
        .with_columns(
            pl.lit(model_name).alias(ModelEvaluation.model),
            pl.lit(level).alias(ModelEvaluation.level),
        )
        .select(
            ModelEvaluation.model,
            ModelEvaluation.level,
            *index,
            ModelEvaluation.metric,
            ModelEvaluation.value,
        )
    )


def score_out_of_fold(
    model_name: str, spec: dict, oof_df: pl.DataFrame, locations_lf: pl.LazyFrame
) -> pl.DataFrame:
    """
    Score held-out predictions against known and imputed filled posts, per fold and pooled.

    Predictions are in filled posts (care home ratios multiplied by beds) and clipped at 1,
    except the "row_level_metadata_scale" level, which scores the raw model output (the ratio
    for care homes). Known-value levels score rows with a known value and a prediction.
    Coverage is those rows over all known rows, so rows without a prediction or beds are
    unscored. Imputed-target levels score every row with a target and a prediction.

    The joined rows are collected once because every level reads them.

    Args:
        model_name (str): the model being scored
        spec (dict): the model's registry entry
        oof_df (pl.DataFrame): out-of-fold predictions from `cv.predict_out_of_fold`
        locations_lf (pl.LazyFrame): one row per location and import date of the model's care
            home type, with known posts, service, CSSR, beds, banded beds and both imputed
            targets. Keys must be unique.

    Returns:
        pl.DataFrame: long-format scores, see `to_long`
    """
    is_ratio_model = (
        spec[MRKeys.dependent] == IndCQC.imputed_filled_posts_per_bed_ratio_model
    )
    results = []

    def add(level, scores_df, group_columns=None, fold_column=None):
        results.append(
            to_long(scores_df, model_name, level, group_columns, fold_column)
        )

    joined_lf = cv.convert_to_filled_posts(oof_df.lazy(), locations_lf, spec).join(
        locations_lf, on=KEYS, how="left", validate="m:1"
    )
    if is_ratio_model:
        joined_lf = joined_lf.join(
            oof_df.lazy().select(*KEYS, pl.col(PRED).alias(RATIO_PRED)),
            on=KEYS,
            how="left",
            validate="m:1",
        )
        size_band = pl.col(IndCQC.number_of_beds_banded).cast(pl.Int32).cast(pl.String)
        target = pl.col(IndCQC.imputed_filled_posts_per_bed_ratio_model) * pl.col(
            IndCQC.number_of_beds
        )
    else:
        band_lf = cv.add_non_res_size_band(
            locations_lf, KNOWN, IndCQC.location_id
        ).select(IndCQC.location_id, BAND)
        joined_lf = joined_lf.join(
            band_lf.unique(), on=IndCQC.location_id, how="left", validate="m:1"
        )
        size_band = pl.col(BAND).cast(pl.String)
        target = pl.col(IndCQC.imputed_filled_post_model)

    joined_lf = evaluation.add_financial_year(
        joined_lf.with_columns(
            pl.col(PRED).alias(UNCLIPPED_PRED),
            pl.col(PRED).clip(lower_bound=1.0),
            size_band.alias(BAND),
            target.cast(pl.Float64).alias(TARGET_POSTS),
        ),
        IndCQC.cqc_location_import_date,
    )
    joined_df = joined_lf.select(
        *KEYS,
        FOLD,
        IndCQC.primary_service_type,
        IndCQC.current_cssr,
        IndCQC.number_of_beds,
        ModelEvaluation.financial_year,
        BAND,
        KNOWN,
        TARGET_POSTS,
        PRED,
        UNCLIPPED_PRED,
        *([RATIO_PRED] if is_ratio_model else []),
    ).collect()

    scored_lf = joined_df.lazy().filter(
        pl.col(KNOWN).is_not_null() & pl.col(PRED).is_not_null()
    )
    known_rows = locations_lf.select(pl.col(KNOWN).is_not_null().sum()).collect().item()
    scored_rows = scored_lf.select(pl.len()).collect().item()
    add(
        Labels.coverage,
        pl.DataFrame(
            {
                ModelEvaluation.known_rows_scored: [scored_rows],
                ModelEvaluation.known_rows: [known_rows],
                ModelEvaluation.coverage: [scored_rows / known_rows],
            }
        ),
    )

    service = IndCQC.primary_service_type
    date = IndCQC.cqc_location_import_date
    financial_year = ModelEvaluation.financial_year
    headline_groups = [service, IndCQC.current_cssr, financial_year]

    def score_groups(groups_lf, by_columns=None):
        scores_lf = evaluation.score_group_totals(groups_lf, PRED, KNOWN, by_columns)
        if by_columns:
            counts_lf = groups_lf.group_by(by_columns).agg(
                pl.len().alias(ModelEvaluation.number_of_groups)
            )
            return scores_lf.join(counts_lf, on=by_columns).collect()
        return scores_lf.join(
            groups_lf.select(pl.len().alias(ModelEvaluation.number_of_groups)),
            how="cross",
        ).collect()

    by_fold_lf = evaluation.aggregate_totals_by_group(
        scored_lf, PRED, KNOWN, [FOLD, *headline_groups]
    )
    pooled_lf = evaluation.aggregate_totals_by_group(
        scored_lf, PRED, KNOWN, headline_groups
    )
    add(
        Labels.headline_group_totals,
        score_groups(by_fold_lf, [FOLD]),
        fold_column=FOLD,
    )
    add(Labels.headline_group_totals, score_groups(pooled_lf))
    add(
        Labels.headline_by_year,
        score_groups(pooled_lf, [financial_year]),
        [financial_year],
    )

    for by_columns in ([FOLD, service], [service]):
        period_totals_lf = evaluation.aggregate_totals_by_group(
            scored_lf, PRED, KNOWN, [*by_columns, date]
        )
        bias_lf = evaluation.calculate_period_bias(
            scored_lf, PRED, KNOWN, date, by_columns
        )
        scores_df = (
            evaluation.score_group_totals(period_totals_lf, PRED, KNOWN, by_columns)
            .join(
                evaluation.fit_bias_slope_per_year(bias_lf, KNOWN, date, by_columns),
                on=by_columns,
            )
            .collect()
        )
        add(
            Labels.period_totals,
            scores_df,
            [service],
            FOLD if FOLD in by_columns else None,
        )

    add(
        Labels.size_band,
        evaluation.score_rows(scored_lf, PRED, KNOWN, [FOLD, BAND]).collect(),
        [BAND],
        FOLD,
    )
    add(
        Labels.size_band,
        evaluation.score_rows(scored_lf, PRED, KNOWN, [BAND]).collect(),
        [BAND],
    )
    add(
        Labels.row_level_posts,
        evaluation.score_rows(scored_lf, PRED, KNOWN, [FOLD]).collect(),
        fold_column=FOLD,
    )
    add(
        Labels.row_level_posts,
        evaluation.score_rows(scored_lf, PRED, KNOWN).collect(),
    )

    scored_df = scored_lf.collect()
    for fold in [*sorted(scored_df[FOLD].unique().to_list()), None]:
        fold_df = scored_df if fold is None else scored_df.filter(pl.col(FOLD) == fold)
        metrics_df = pl.DataFrame(
            {
                metric: [value]
                for metric, value in _metadata_scale_metrics(
                    fold_df, model_name, is_ratio_model
                ).items()
            }
        )
        if fold is not None:
            metrics_df = metrics_df.with_columns(pl.lit(fold).alias(FOLD))
        add(
            Labels.row_level_metadata_scale,
            metrics_df,
            fold_column=FOLD if fold is not None else None,
        )

    target_lf = joined_df.lazy().filter(
        pl.col(TARGET_POSTS).is_not_null() & pl.col(PRED).is_not_null()
    )
    add(
        Labels.imputed_target_rows,
        evaluation.score_rows(target_lf, PRED, TARGET_POSTS, [FOLD]).collect(),
        fold_column=FOLD,
    )
    add(
        Labels.imputed_target_rows,
        evaluation.score_rows(target_lf, PRED, TARGET_POSTS).collect(),
    )
    for by_columns in ([FOLD, service], [service]):
        bias_lf = evaluation.calculate_period_bias(
            target_lf, PRED, TARGET_POSTS, date, by_columns
        )
        slopes_df = evaluation.fit_bias_slope_per_year(
            bias_lf, TARGET_POSTS, date, by_columns
        ).collect()
        add(
            Labels.imputed_target_period_bias,
            slopes_df,
            [service],
            FOLD if FOLD in by_columns else None,
        )

    return pl.concat(results)


def _metadata_scale_metrics(
    df: pl.DataFrame, model_name: str, is_ratio_model: bool
) -> dict:
    """Row-level R2, RMSE and proportions within 10 and 25, on the ratio for care homes."""
    if is_ratio_model:
        df = df.filter(pl.col(IndCQC.number_of_beds).is_not_null())
        beds = df[IndCQC.number_of_beds].to_numpy()
        return model_utils.calculate_metrics(
            (df[KNOWN] / df[IndCQC.number_of_beds]).to_numpy(),
            df[RATIO_PRED].to_numpy(),
            IndCQC.care_home_model,
            beds,
        )

    return model_utils.calculate_metrics(
        df[KNOWN].to_numpy(), df[UNCLIPPED_PRED].to_numpy(), model_name
    )


def describe_fit(
    model_name: str, model: LinearRegression | Pipeline, features: list[str]
) -> tuple[pl.DataFrame, dict[str, float]]:
    """
    Report whether a fit converged and how many coefficients it zeroed.

    Args:
        model_name (str): the model being described
        model (LinearRegression | Pipeline): the fitted model
        features (list[str]): feature names, in the order the model was fitted on

    Returns:
        tuple[pl.DataFrame, dict[str, float]]: long-format fit diagnostics (iterations used
            and allowed, whether the limit was hit (always 0 for models that don't iterate),
            zeroed coefficients and feature count), and each feature's coefficient
    """
    estimator = model[-1] if isinstance(model, Pipeline) else model
    coefficients = {f: float(c) for f, c in zip(features, estimator.coef_)}
    n_iter = int(np.max(getattr(estimator, "n_iter_", 0)))
    max_iter = getattr(estimator, "max_iter", 0)

    scores_df = pl.DataFrame(
        {
            FOLD: [Labels.all_rows],
            ModelEvaluation.n_iter: [n_iter],
            ModelEvaluation.max_iter: [max_iter],
            ModelEvaluation.hit_max_iter: [float(max_iter > 0 and n_iter >= max_iter)],
            ModelEvaluation.zero_coefficients: [
                sum(c == 0 for c in coefficients.values())
            ],
            ModelEvaluation.number_of_features: [len(features)],
        }
    )

    return (
        to_long(scores_df, model_name, Labels.fit_diagnostics, fold_column=FOLD),
        coefficients,
    )


def score_jumpiness(
    model_name: str,
    spec: dict,
    model: LinearRegression | Pipeline,
    features_lf: pl.LazyFrame,
    folds_lf: pl.LazyFrame,
    locations_lf: pl.LazyFrame,
    seed: int,
    sample_size: int,
) -> pl.DataFrame:
    """
    Measure how much predictions move between periods for locations with no target at any date.

    These locations (no ASC-WDS and no PIR) are published from the model alone. A seeded
    sample is predicted with the all-rows model, clipped at 1. Jumpiness is the mean absolute
    change in log(prediction) between consecutive periods, so about the average % change.

    Args:
        model_name (str): the model being scored
        spec (dict): the model's registry entry
        model (LinearRegression | Pipeline): the model fitted on all rows
        features_lf (pl.LazyFrame): features dataset, including the dependent column
        folds_lf (pl.LazyFrame): fold for each location, used to predict in chunks
        locations_lf (pl.LazyFrame): beds for each location and import date
        seed (int): seed for choosing the sample
        sample_size (int): the most locations to sample

    Returns:
        pl.DataFrame: long-format jumpiness, with the locations and rows sampled
    """
    dependent = spec[MRKeys.dependent]
    location = IndCQC.location_id
    candidates = (
        evaluation.add_never_submitted_flag(
            features_lf.select(location, IndCQC.care_home_status_count, dependent),
            dependent,
            location,
        )
        .filter(
            pl.col(ModelEvaluation.never_submitted)
            & (pl.col(IndCQC.care_home_status_count) == 1)
        )
        .select(location)
        .unique()
        .sort(location)
        .collect()[location]
        .to_list()
    )
    sample = np.random.default_rng(seed).permutation(candidates)[:sample_size]
    sample_lf = pl.LazyFrame({location: sample.tolist()}, schema={location: pl.String})

    predictions_lf = cv.convert_to_filled_posts(
        cv.predict_in_chunks(
            model,
            features_lf.join(sample_lf, on=location, how="semi"),
            folds_lf.join(sample_lf, on=location, how="semi"),
            spec,
        ).lazy(),
        locations_lf,
        spec,
    ).with_columns(
        pl.col(PRED).cast(pl.Float64).clip(lower_bound=1.0).log().alias(LOG_PRED)
    )

    jumpiness_df = evaluation.mean_period_to_period_change(
        predictions_lf, [LOG_PRED], [location], IndCQC.cqc_location_import_date
    ).collect()
    counts_df = predictions_lf.select(
        pl.col(location).n_unique().alias(ModelEvaluation.locations),
        pl.len().alias(ModelEvaluation.rows),
    ).collect()

    return to_long(
        jumpiness_df.drop(ModelEvaluation.column_name).hstack(counts_df),
        model_name,
        Labels.jumpiness,
    )
