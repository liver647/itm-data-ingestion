"""A module containing the local spark session utils."""

import logging, re

import delta
from pyspark import SparkConf
from pyspark.sql import SparkSession

from itm_data_ingestion.infrastructure.settings import Settings


def get_spark_session(
        config: Settings
) -> SparkSession:
    """
    Get the spark session.

    Returns the active spark session if it already exists, otherwise creates a new one.

    Returns
    -------
    SparkSession
        The spark session.
    """
    py4jlogger = logging.getLogger("py4j")
    py4jlogger.setLevel(logging.WARNING)

    app_name = config.spark_app_name
    account_name = re.search(r"AccountName=([^;]+)", config.azure_storage_connection_string).group(1)
    account_key  = re.search(r"AccountKey=([^;]+)",  config.azure_storage_connection_string).group(1)

    conf = (
        SparkConf()
        # General Configuration
        .set("spark.sql.session.timeZone", "UTC")
        .set("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
        .set("spark.sql.parquet.compression.codec", "snappy")
        .set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .set("spark.sql.legacy.timeParserPolicy", "CORRECTED")
        .set("spark.sql.optimizer.dynamicPartitionPruning.enabled", "true")
        .set("spark.sql.join.preferSortMergeJoin", "true")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        # Enable Adaptive Query Execution
        .set("spark.sql.adaptive.enabled", "true")
        .set("spark.sql.adaptive.skewJoin.enabled", "true")
        .set("spark.sql.adaptive.coalescePartitions.enabled", "true")
        # Enable Delta Lake
        .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    
    )
    builder = (
        SparkSession.builder
        .appName(app_name)
        .config(conf=conf)
        .enableHiveSupport()
    )
    
    spark = delta.configure_spark_with_delta_pip(builder).getOrCreate()
    spark.conf.set(
        f"fs.azure.account.key.{account_name}.blob.core.windows.net",
        account_key,
    )
    
    spark.sparkContext.setLogLevel("INFO")

    return spark