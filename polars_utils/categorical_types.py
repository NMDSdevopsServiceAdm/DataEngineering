"""Shared Polars Categorical dtypes reused across pipelines.

`EstablishmentCatType` is defined once here so pipelines that key on
`establishment_id` can compare/join without recasting, rather than each
pipeline defining its own `Categories("establishment", ...)` instance.
"""

import polars as pl

EstablishmentCatType = pl.Categorical(
    pl.Categories("establishment", namespace="filled_posts")
)
JobRoleCatType = pl.Categorical(pl.Categories("job_role", namespace="slv"))
