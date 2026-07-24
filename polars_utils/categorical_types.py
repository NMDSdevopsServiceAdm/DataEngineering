import polars as pl

EstablishmentCatType = pl.Categorical(
    pl.Categories("establishment", namespace="filled_posts")
)
