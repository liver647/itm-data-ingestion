"""A module containing the logger utils."""

from typing import Protocol

from pyspark.sql import SparkSession


class Logger(Protocol):
    """A protocol for the logger."""

    def info(self, message: str) -> None:
        """Log an info message."""

    def error(self, message: str) -> None:
        """Log an error message."""

    def warn(self, message: str) -> None:
        """Log a warning message."""


def get_logger(spark: SparkSession, prefix: str) -> Logger:
    """
    Get the py4j logger with the given prefix.

    Parameters
    ----------
    spark: SparkSession
        Spark session instance.

    prefix: str
        Prefix to use for the logger.

    Returns
    -------
    Logger
        The logger instance.
    """
    # pylint: disable=protected-access
    log4j_logger = spark.sparkContext._jvm.org.apache.log4j  # type: ignore[union-attr]
    return log4j_logger.LogManager.getLogger("__USER_LOGS__" + prefix)  # type: ignore[no-any-return]
