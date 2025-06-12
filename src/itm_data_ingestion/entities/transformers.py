"""A module containing the Project transformations."""

from pyspark.sql import SparkSession, DataFrame
from itm_data_ingestion.infrastructure.settings import Settings
from pyspark.sql.functions import concat_ws, to_timestamp, col, split, regexp_replace

from typing import Final

class Transformers:
    """
    Gather all needed data transformations
    """

    TIMESTAMP_FMT: Final[str] = "yyyy-MM-dd HH mm"

    def __init__(self, config : Settings):
        self.config = config

    def handle_stores (self, input_stores_df: DataFrame) -> DataFrame :
        """
        Handle stores data from client, extract latitude & longitude
        & generate output daframe to be loaded
        """
        cleaned_input_stores_df = input_stores_df.withColumn("clean_latlng", regexp_replace(col("latlng"), "[()]", ""))

        return (
            cleaned_input_stores_df
            .withColumn("latitude", split(col("clean_latlng"),",").getItem(0).cast("double"))
            .withColumn("longitude", split(col("clean_latlng"),",").getItem(1).cast("double"))
            .withColumnRenamed("opening","opening_hour")
            .withColumnRenamed("closing","closing_hour")
            .drop("latlng")
            .drop("clean_latlng")
        )
    
    def handle_transactions (self, input_transactions_df: DataFrame, clients_df: DataFrame) -> DataFrame :
        """
        Handle transactions data from client, create complete timestamp field
        & get account_id
        """
        enriched_transctions_df = (
            input_transactions_df.join(
                clients_df.select("id", "account_id"),
                col("client_id") == col("id"),
                how="left"
            )
            .withColumn(
                "timestamp_utc",
                to_timestamp(
                    concat_ws(" ", col("date"), col("hour"), col("minute")),
                    self.TIMESTAMP_FMT
                )
            )
            .drop("date", "hour", "minute", "id")
        )

        return enriched_transctions_df