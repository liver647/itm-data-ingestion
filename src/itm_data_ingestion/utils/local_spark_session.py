"""A module containing the local spark session utils."""

import logging

import delta
from pyspark import SparkConf
from pyspark.sql import SparkSession


def get_spark_session() -> SparkSession:
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

    app_name = "New Client Data Ingestion Job Application"

    conf = SparkConf().setAll(
        [
            # General Configuration
            ("spark.sql.sources.partitionColumnTypeInference.enabled", "false"),
            ("spark.sql.session.timeZone", "UTC"),
            ("spark.sql.parquet.compression.codec", "snappy"),
            ("spark.sql.sources.partitionOverwriteMode", "dynamic"),
            ("spark.sql.legacy.timeParserPolicy", "CORRECTED"),
            ("spark.sql.optimizer.dynamicPartitionPruning.enabled", "true"),
            ("spark.sql.join.preferSortMergeJoin", "true"),
            ("spark.serializer", "org.apache.spark.serializer.KryoSerializer"),
            # Enable Adaptive Query Execution
            ("spark.sql.adaptive.enabled", "true"),
            ("spark.sql.adaptive.skewJoin.enabled", "true"),
            ("spark.sql.adaptive.coalescePartitions.enabled", "true"),
            # Enable Delta Lake
            ("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"),
            ("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"),
            # Apply Auto Broadcast Join if the smallest dataframe is less than 25 MB
            ("spark.sql.autoBroadcastJoinThreshold", "26214400"),
            ("spark.sql.adaptive.autoBroadcastJoinThreshold", "26214400"),
            # Writing Optimization
            ("spark.databricks.delta.optimizeWrite.enabled", "true"),
            ("spark.databricks.delta.optimizeWrite.binSize", "32"),  # 32MB
            ("spark.databricks.delta.autoCompact.enabled", "true"),
            ("spark.databricks.delta.autoCompact.maxFileSize", "268435456"),  # 256 MB
            # Hadoop Configuration
            ("spark.hadoop.parquet.block.size", str(1024 * 1024 * 16)),  # 16MB
        ]
    )

    builder = SparkSession.builder.config(conf=conf).appName(app_name).enableHiveSupport()
    spark = delta.configure_spark_with_delta_pip(builder).getOrCreate()

    spark.sparkContext.setLogLevel("INFO")

    return spark