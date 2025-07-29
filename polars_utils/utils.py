import polars as pl
import logging

util_logger = logging.getLogger(__name__)
util_logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
util_logger.addHandler(logging.StreamHandler())
util_logger.handlers[0].setFormatter(formatter)


def write_to_parquet(
    df: pl.DataFrame,
    output_path: str,
    logger: logging.Logger=util_logger,
    partition_keys=None
) -> None:
    try:
        if df.height == 0:
            logger.info('The provided dataframe was empty. No data was written.')
        else:
            df.write_parquet(output_path, partition_by=partition_keys)
            logger.info('Parquet written to {}'.format(output_path))
    except FileNotFoundError:
        logger.error('The destination path is invalid')