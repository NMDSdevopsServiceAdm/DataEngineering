import logging
import sys


def get_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    """Provides a logger for the supplied name with standardardised formatting and output

    Args:
        name (str): the name of the logger (typically the `__name__` of the calling module)
        level (int, optional): the logging level. Defaults to logging.INFO.

    Returns:
        logging.Logger: the Logger object which controls logging
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    return logger
