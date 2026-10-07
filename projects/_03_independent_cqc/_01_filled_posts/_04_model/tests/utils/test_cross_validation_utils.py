from datetime import date

import numpy as np
import polars as pl
import pytest
from sklearn.linear_model import LinearRegression

from projects._03_independent_cqc._01_filled_posts._04_model.utils import (
    cross_validation_utils as job,
)
from projects._03_independent_cqc._01_filled_posts.unittest_data.polars_ind_cqc_test_file_data import (
    CrossValidationUtilsData as Data,
)
from projects._03_independent_cqc._01_filled_posts.unittest_data.polars_ind_cqc_test_file_data import (
    cross_validation_features_data,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.ind_cqc_pipeline_columns import (
    ModelEvaluationColumns as ModelEvaluation,
)
from utils.column_names.ind_cqc_pipeline_columns import ModelRegistryKeys as MRKeys

LOCATION_ROWS = 3


def features_lf(**overrides_by_column) -> pl.LazyFrame:
    """The features data, with some locations' values replaced in a column.

    Each override is a dict of location ID to the value for all of its rows.
    """
    data = cross_validation_features_data()
    for column, overrides in overrides_by_column.items():
        data[column] = [
            overrides.get(location, value)
            for location, value in zip(data[IndCQC.location_id], data[column])
        ]
    return pl.LazyFrame(data)


def folds_lf() -> pl.LazyFrame:
    return pl.LazyFrame(
        Data.folds_data, schema_overrides={ModelEvaluation.fold: pl.UInt8}
    )


def locations_predictions(
    returned_df: pl.DataFrame, locations: list[str]
) -> tuple[np.ndarray, np.ndarray]:
    """The returned predictions for some locations, and what the pattern says they should be."""
    exact_df = pl.DataFrame(cross_validation_features_data()).select(
        IndCQC.location_id,
        IndCQC.cqc_location_import_date,
        pl.col(IndCQC.imputed_filled_post_model).alias("exact"),
    )
    joined_df = returned_df.join(
        exact_df, on=[IndCQC.location_id, IndCQC.cqc_location_import_date]
    ).filter(pl.col(IndCQC.location_id).is_in(locations))

    return joined_df[IndCQC.prediction].to_numpy(), joined_df["exact"].to_numpy()


class TestFitModel:
    def test_fits_the_model_type_in_the_spec(self):
        train_df = pl.DataFrame(cross_validation_features_data())

        model = job.fit_model(train_df, Data.spec)

        assert isinstance(model, LinearRegression)

    def test_features_used_in_the_order_of_the_spec(self):
        train_df = pl.DataFrame(cross_validation_features_data())
        model = job.fit_model(train_df, Data.spec)

        # The spec lists the activity count first, which has a coefficient of 3.
        np.testing.assert_allclose(model.coef_, [3.0, 2.0])


class TestPredictOutOfFold:
    def test_every_row_predicted_once_with_its_fold(self):
        returned_df = job.predict_out_of_fold(features_lf(), folds_lf(), Data.spec)

        assert returned_df.height == len(Data.folds_data[IndCQC.location_id]) * (
            LOCATION_ROWS
        )
        assert not (
            returned_df.select(IndCQC.location_id, IndCQC.cqc_location_import_date)
            .is_duplicated()
            .any()
        )
        assert returned_df[IndCQC.prediction].null_count() == 0
        assert returned_df[IndCQC.prediction].dtype == pl.Float32

    def test_predictions_follow_the_pattern_when_every_location_follows_it(self):
        returned_df = job.predict_out_of_fold(features_lf(), folds_lf(), Data.spec)

        predicted, exact = locations_predictions(
            returned_df, Data.folds_data[IndCQC.location_id]
        )
        np.testing.assert_allclose(predicted, exact, rtol=1e-4)

    def test_location_predicted_by_a_model_that_never_saw_it(self):
        # loc2 is in fold 1, so only the fold 1 model is untrained on its rows.
        garbage_lf = features_lf(
            **{IndCQC.imputed_filled_post_model: {"loc2": Data.garbage_dependent}}
        )

        returned_df = job.predict_out_of_fold(garbage_lf, folds_lf(), Data.spec)

        held_out, held_out_exact = locations_predictions(returned_df, ["loc2"])
        trained_on_it, trained_on_it_exact = locations_predictions(
            returned_df, ["loc0", "loc1", "loc4", "loc5"]
        )
        np.testing.assert_allclose(held_out, held_out_exact, rtol=1e-4)
        # The other folds' models did train on loc2, so they are visibly thrown off by it.
        assert not np.allclose(trained_on_it, trained_on_it_exact, rtol=1e-2)

    def test_rows_without_a_dependent_are_predicted_but_not_trained_on(self):
        missing_lf = features_lf(**{IndCQC.imputed_filled_post_model: {"loc1": None}})

        returned_df = job.predict_out_of_fold(missing_lf, folds_lf(), Data.spec)

        predicted, exact = locations_predictions(
            returned_df, Data.folds_data[IndCQC.location_id]
        )
        assert returned_df[IndCQC.prediction].null_count() == 0
        np.testing.assert_allclose(predicted, exact, rtol=1e-4)

    def test_locations_that_changed_care_home_status_are_not_trained_on(self):
        changed_lf = features_lf(
            **{
                IndCQC.care_home_status_count: {"loc3": 2},
                IndCQC.imputed_filled_post_model: {"loc3": Data.garbage_dependent},
            }
        )

        returned_df = job.predict_out_of_fold(changed_lf, folds_lf(), Data.spec)

        # loc3 is in fold 1: its garbage dependent would throw off folds 0 and 2 if used.
        predicted, exact = locations_predictions(
            returned_df, ["loc0", "loc1", "loc4", "loc5"]
        )
        np.testing.assert_allclose(predicted, exact, rtol=1e-4)

    def test_raises_when_a_location_has_no_fold(self):
        folds_missing_a_location_lf = folds_lf().filter(
            pl.col(IndCQC.location_id) != "loc5"
        )

        with pytest.raises(ValueError, match="1 locations have no fold"):
            job.predict_out_of_fold(
                features_lf(), folds_missing_a_location_lf, Data.spec
            )


class TestPredictInChunks:
    def test_every_row_predicted_by_a_model_fitted_on_training_rows_only(self):
        changed_lf = features_lf(
            **{
                IndCQC.care_home_status_count: {"loc3": 2},
                IndCQC.imputed_filled_post_model: {
                    "loc3": Data.garbage_dependent,
                    "loc1": None,
                },
            }
        )
        model = job.fit_on_all_rows(changed_lf, Data.spec)

        returned_df = job.predict_in_chunks(model, changed_lf, folds_lf(), Data.spec)

        predicted, exact = locations_predictions(
            returned_df, Data.folds_data[IndCQC.location_id]
        )
        assert returned_df.height == (
            len(Data.folds_data[IndCQC.location_id]) * LOCATION_ROWS
        )
        np.testing.assert_allclose(predicted, exact, rtol=1e-4)

    def test_raises_when_a_location_has_no_fold(self):
        folds_missing_a_location_lf = folds_lf().filter(
            pl.col(IndCQC.location_id) != "loc5"
        )
        model = job.fit_on_all_rows(features_lf(), Data.spec)

        with pytest.raises(ValueError, match="1 locations have no fold"):
            job.predict_in_chunks(
                model, features_lf(), folds_missing_a_location_lf, Data.spec
            )


class TestConvertToFilledPosts:
    @pytest.mark.parametrize(
        "case", [c.as_pytest_param() for c in Data.filled_posts_conversion_test_cases]
    )
    def test_ratios_converted_and_posts_left_alone(self, case):
        locations = [f"loc{i}" for i in range(len(case.predictions))]
        import_dates = [date(2024, 1, 1)] * len(locations)
        keys = {
            IndCQC.location_id: locations,
            IndCQC.cqc_location_import_date: import_dates,
        }
        predictions_lf = pl.LazyFrame(
            {**keys, IndCQC.prediction: case.predictions},
            schema_overrides={IndCQC.prediction: pl.Float32},
        )
        number_of_beds_lf = pl.LazyFrame(
            {**keys, IndCQC.number_of_beds: case.number_of_beds}
        )
        spec = {**Data.spec, MRKeys.dependent: case.dependent}

        returned_df = job.convert_to_filled_posts(
            predictions_lf, number_of_beds_lf, spec
        ).collect()

        assert returned_df[IndCQC.prediction].to_list() == case.expected_predictions
        assert returned_df.columns == [
            IndCQC.location_id,
            IndCQC.cqc_location_import_date,
            IndCQC.prediction,
        ]


class TestAddNonResSizeBand:
    @pytest.mark.parametrize(
        "case", [c.as_pytest_param() for c in Data.non_res_size_band_test_cases]
    )
    def test_bands_from_each_locations_mean_known_posts(self, case):
        input_lf = pl.LazyFrame(
            {
                IndCQC.location_id: case.location_ids,
                IndCQC.ascwds_filled_posts_dedup_clean: case.known_posts,
            }
        )

        returned_df = job.add_non_res_size_band(
            input_lf, IndCQC.ascwds_filled_posts_dedup_clean, IndCQC.location_id
        ).collect()

        assert (
            returned_df[ModelEvaluation.size_band].cast(pl.String).to_list()
            == case.expected_bands
        )
