import polars as pl


def preprocess_non_res_pir(path_to_data: str, lazy=False) -> None:
    data = pl.scan_parquet(path_to_data) if lazy else pl.read_parquet(path_to_data)
    return
