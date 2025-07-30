import polars as pl
import logging
from typing import Union

util_logger = logging.getLogger(__name__)
util_logger.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
util_logger.addHandler(logging.StreamHandler())
util_logger.handlers[0].setFormatter(formatter)


def write_to_parquet(
    df: pl.DataFrame,
    output_path: str,
    logger: logging.Logger = util_logger,
    partition_keys: Union[list[str] , None] =None,
) -> None:
    """Writes a Polars DataFrame to a Parquet file.

        If the DataFrame is empty, an informational message is logged, and no file
        is written. Otherwise, the DataFrame is written to the specified path,
        optionally partitioned by the given keys.

        Args:
            df (pl.DataFrame): The Polars DataFrame to write.
            output_path (str): The file path where the Parquet file(s) will be written.
                This can be a directory if `partition_keys` are provided.
            logger (logging.Logger): An optional logger instance to use for logging messages.
                If not provided, a default logger will be used (or you can ensure
                `util_logger` is globally available).
            partition_keys (Union[list[str] , None]): An optional list of column names to partition the
                Parquet file(s) by. If provided, `output_path` should be a directory.
                Defaults to None, meaning no partitioning.

        Return:
            None
    """
    if df.height == 0:
        logger.info("The provided dataframe was empty. No data was written.")
    else:
        df.write_parquet(output_path, partition_by=partition_keys)
        logger.info("Parquet written to {}".format(output_path))
