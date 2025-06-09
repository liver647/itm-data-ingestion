"""A module containing the Project transformations."""

from __future__ import annotations

from pyspark.sql import SparkSession, DataFrame
from itm_data_ingestion.infrastructure.settings import Settings
from pyspark.sql.functions import concat_ws, to_timestamp, col, split, regexp_replace

from typing import Final

class Transformers:
    def __init__(self, config : Settings):
        self.config = config

    def handle_stores (self, input_stores: DataFrame) -> DataFrame :
        """
        Handle stores data from client, extract latitude & longitude
        & generate output daframe to be loaded
        """
        cleaned_input_stores = input_stores.withColumn("clean_latlng", regexp_replace(col("latlng"), "[()]", ""))

        return (
            cleaned_input_stores
            .withColumn("latitude", split(col("clean_latlng"),",").getItem(0).cast("double"))
            .withColumn("longitude", split(col("clean_latlng"),",").getItem(1).cast("double"))
            .drop("latlng")
            .drop("clean_latlng")
        )