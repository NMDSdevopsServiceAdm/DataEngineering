import polars as pl


def test_polars_engine_affinity_is_in_memory():
    assert pl.Config.state().get("POLARS_ENGINE_AFFINITY") == "in-memory"
