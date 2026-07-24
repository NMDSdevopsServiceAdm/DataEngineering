import polars as pl

JobRoleCatType = pl.Categorical(pl.Categories("job_role", namespace="filled_posts"))
