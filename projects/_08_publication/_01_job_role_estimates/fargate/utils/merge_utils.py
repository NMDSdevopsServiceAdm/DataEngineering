import polars as pl


def join_estimates_and_metadata(
    estimates_lf: pl.LazyFrame,
    metadata_lf: pl.LazyFrame,
) -> pl.LazyFrame:
    """
    Placeholder: joins the archived job role estimates and metadata LazyFrames.

    Args:
        estimates_lf (pl.LazyFrame): The archived job role estimates LazyFrame.
        metadata_lf (pl.LazyFrame): The archived job role metadata LazyFrame.

    Returns:
        pl.LazyFrame: estimates_lf, unchanged.
    """
    return estimates_lf


def join_geography(
    merged_lf: pl.LazyFrame,
    geography_lf: pl.LazyFrame,
) -> pl.LazyFrame:
    """
    Placeholder: joins the archived geography LazyFrame into the merged data.

    Args:
        merged_lf (pl.LazyFrame): The estimates and metadata, already merged.
        geography_lf (pl.LazyFrame): The archived geography LazyFrame.

    Returns:
        pl.LazyFrame: merged_lf, unchanged.
    """
    return merged_lf
