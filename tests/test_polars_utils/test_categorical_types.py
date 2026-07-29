import polars as pl
import polars.testing as pl_testing

import polars_utils.categorical_types as job
from projects._03_independent_cqc._07_estimate_filled_posts_by_job_role.fargate.utils.utils import (
    CategoricalColumnTypes as IndCQCCatColType,
)


class TestEstablishmentCatType:
    def test_has_expected_categories_name_and_namespace(self):
        categories = job.EstablishmentCatType.categories

        assert categories.name() == "establishment"
        assert categories.namespace() == "filled_posts"

    def test_joins_with_ind_cqc_establishment_cat_type_without_recast(self):
        left_lf = pl.LazyFrame(
            {"establishment_id": ["1", "2"]},
            schema={"establishment_id": job.EstablishmentCatType},
        )
        right_lf = pl.LazyFrame(
            {"establishment_id": ["1", "2"], "value": [10, 20]},
            schema={
                "establishment_id": IndCQCCatColType.EstablishmentCatType,
                "value": pl.Int32,
            },
        )

        returned_lf = left_lf.join(right_lf, on="establishment_id", how="left")

        expected_lf = pl.LazyFrame(
            {"establishment_id": ["1", "2"], "value": [10, 20]},
            schema={
                "establishment_id": job.EstablishmentCatType,
                "value": pl.Int32,
            },
        )
        pl_testing.assert_frame_equal(expected_lf, returned_lf, check_row_order=False)


class TestJobRoleCatType:
    def test_distinct_from_establishment_cat_type(self):
        assert job.JobRoleCatType != job.EstablishmentCatType

    def test_distinct_from_ind_cqc_job_role_enum_type(self):
        assert job.JobRoleCatType != IndCQCCatColType.JobRoleEnumType
