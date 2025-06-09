"""A module containing the Delta writer."""

from pyspark.sql import SparkSession, DataFrame
from itm_data_ingestion.infrastructure.settings import Settings

class DeltaWriter:
    def __init__(self, config: Settings):
        self.config = config

    def write(self, name: str, df: DataFrame):
        target = self.config.delta_dir / name
        (
            df.write.format("delta")
            .mode("overwrite")
            .save(str(target))
        )