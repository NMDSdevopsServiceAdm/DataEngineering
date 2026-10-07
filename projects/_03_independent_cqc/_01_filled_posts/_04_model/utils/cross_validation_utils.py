import polars as pl
from sklearn.linear_model import LinearRegression
from sklearn.pipeline import Pipeline

from projects._03_independent_cqc._01_filled_posts._04_model.utils import (
    model_utils,
    training_utils,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.ind_cqc_pipeline_columns import (
    ModelEvaluationColumns as ModelEvaluation,
)
from utils.column_names.ind_cqc_pipeline_columns import ModelRegistryKeys as MRKeys


def fit_model(train_df: pl.DataFrame, spec: dict) -> LinearRegression | Pipeline:
    """
    Fit the model a spec describes.

    Args:
        train_df (pl.DataFrame): training rows containing the spec's features and dependent
        spec (dict): a model registry entry, or a copy of one with values changed

    Returns:
        LinearRegression | Pipeline: the fitted model
    """
    features, dependent = spec[MRKeys.features], spec[MRKeys.dependent]
    X, y = training_utils.convert_dataframe_to_numpy(train_df, features, dependent)

    model = model_utils.build_model(spec[MRKeys.model_type], spec[MRKeys.model_params])

    return model.fit(X, y)


def fit_on_all_rows(
    features_lf: pl.LazyFrame, spec: dict
) -> LinearRegression | Pipeline:
    """
    Fit the model a spec describes on every row the production job would train on.

    Args:
        features_lf (pl.LazyFrame): features dataset
        spec (dict): a model registry entry, or a copy of one with values changed

    Returns:
        LinearRegression | Pipeline: the fitted model
    """
    train_df = (
        features_lf.filter(training_utils.is_training_row(spec[MRKeys.dependent]))
        .select(*spec[MRKeys.features], spec[MRKeys.dependent])
        .collect()
    )

    return fit_model(train_df, spec)


def predict_out_of_fold(
    features_lf: pl.LazyFrame, folds_lf: pl.LazyFrame, spec: dict
) -> pl.DataFrame:
    """
    Predict every row from a model that was not trained on its location.

    For each fold, a model is trained on the other folds' training rows and predicts all
    rows of the held-out fold. A prediction is therefore never made by a model that has seen
    that row's location. Training rows are the production training job's.

    The selected columns are collected once because scikit-learn needs the data in memory,
    and every fold trains on four fifths of it. It fails if a location has no fold.

    Args:
        features_lf (pl.LazyFrame): features dataset
        folds_lf (pl.LazyFrame): a fold for each location, such as from
            `assign_location_folds`
        spec (dict): a model registry entry, or a copy of one with values changed

    Returns:
        pl.DataFrame: a row per features row, with its location ID, import date, fold and
            "prediction" (the model's raw output, so a ratio for care homes)
    """
    features_df = _add_folds(features_lf, folds_lf, spec).collect()

    predictions = []
    for fold in sorted(features_df[ModelEvaluation.fold].unique().to_list()):
        is_held_out = pl.col(ModelEvaluation.fold) == fold
        train_df = features_df.filter(
            ~is_held_out & training_utils.is_training_row(spec[MRKeys.dependent])
        )
        model = fit_model(train_df, spec)
        predictions.append(_predict_rows(model, features_df.filter(is_held_out), spec))

    return pl.concat(predictions)


def predict_in_chunks(
    model: LinearRegression | Pipeline,
    features_lf: pl.LazyFrame,
    folds_lf: pl.LazyFrame,
    spec: dict,
) -> pl.DataFrame:
    """
    Predict every row, collecting one fold of locations at a time to limit memory.

    It fails if a location has no fold.

    Args:
        model (LinearRegression | Pipeline): a fitted model
        features_lf (pl.LazyFrame): features dataset
        folds_lf (pl.LazyFrame): a fold for each location, used only to split the rows
        spec (dict): the model's registry-style entry, to know which features to use

    Returns:
        pl.DataFrame: a row per features row, with its location ID, import date, fold and
            "prediction" (the model's raw output, so a ratio for care homes)
    """
    features_with_folds_lf = _add_folds(features_lf, folds_lf, spec)
    folds = (
        folds_lf.select(ModelEvaluation.fold)
        .unique()
        .sort(ModelEvaluation.fold)
        .collect()[ModelEvaluation.fold]
        .to_list()
    )

    return pl.concat(
        _predict_rows(
            model,
            features_with_folds_lf.filter(
                pl.col(ModelEvaluation.fold) == fold
            ).collect(),
            spec,
        )
        for fold in folds
    )


def convert_to_filled_posts(
    predictions_lf: pl.LazyFrame, number_of_beds_lf: pl.LazyFrame, spec: dict
) -> pl.LazyFrame:
    """
    Convert predictions to filled posts, multiplying a per bed ratio by the number of beds.

    Predictions of any other dependent are already filled posts and are returned unchanged.

    Args:
        predictions_lf (pl.LazyFrame): predictions, such as from `predict_out_of_fold`
        number_of_beds_lf (pl.LazyFrame): "number_of_beds" for each location and import date
        spec (dict): the model's registry-style entry, to know its dependent

    Returns:
        pl.LazyFrame: predictions with "prediction" in filled posts
    """
    if spec[MRKeys.dependent] != IndCQC.imputed_filled_posts_per_bed_ratio_model:
        return predictions_lf

    keys = [IndCQC.location_id, IndCQC.cqc_location_import_date]

    return (
        predictions_lf.join(
            number_of_beds_lf.select(*keys, IndCQC.number_of_beds), on=keys, how="left"
        )
        .with_columns(
            pl.col(IndCQC.prediction)
            .mul(pl.col(IndCQC.number_of_beds))
            .cast(pl.Float32)
        )
        .drop(IndCQC.number_of_beds)
    )


def add_non_res_size_band(
    lf: pl.LazyFrame, known_column: str, location_column: str
) -> pl.LazyFrame:
    """
    Band each non-res location by its mean known filled posts across all dates.

    The band comes from the known value, so it is outcome-based. Small bands tend to be
    over-predicted and large bands under-predicted by any model.

    Args:
        lf (pl.LazyFrame): dataset containing the known and location columns
        known_column (str): the known filled posts column
        location_column (str): the location ID

    Returns:
        pl.LazyFrame: dataset with "size_band" added, null for locations with no known value
    """
    breaks = [25, 50, 75, 100]
    labels = ["1-24", "25-49", "50-74", "75-99", "100+"]

    return lf.with_columns(
        pl.col(known_column)
        .mean()
        .over(location_column)
        .cut(breaks, labels=labels, left_closed=True)
        .alias(ModelEvaluation.size_band)
    )


def _add_folds(
    features_lf: pl.LazyFrame, folds_lf: pl.LazyFrame, spec: dict
) -> pl.LazyFrame:
    """Select the columns the spec needs and add each location's fold, checking none is missing."""
    keys = [IndCQC.location_id, IndCQC.cqc_location_import_date]
    columns = list(
        dict.fromkeys(
            [
                *keys,
                IndCQC.care_home_status_count,
                spec[MRKeys.dependent],
                *spec[MRKeys.features],
            ]
        )
    )
    locations_without_a_fold = (
        features_lf.select(IndCQC.location_id)
        .unique()
        .join(folds_lf, on=IndCQC.location_id, how="anti")
        .select(pl.len())
        .collect()
        .item()
    )
    if locations_without_a_fold:
        raise ValueError(f"{locations_without_a_fold} locations have no fold")

    return features_lf.select(columns).join(
        folds_lf.select(IndCQC.location_id, ModelEvaluation.fold),
        on=IndCQC.location_id,
        how="left",
    )


def _predict_rows(
    model: LinearRegression | Pipeline, df: pl.DataFrame, spec: dict
) -> pl.DataFrame:
    """Predict each row, keeping the columns that identify it."""
    predictions = model.predict(df.select(spec[MRKeys.features]).to_numpy())

    return df.select(
        IndCQC.location_id, IndCQC.cqc_location_import_date, ModelEvaluation.fold
    ).with_columns(pl.Series(IndCQC.prediction, predictions, dtype=pl.Float32))
