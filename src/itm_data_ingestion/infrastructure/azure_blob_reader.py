"""A module containing the Azure Loader."""


from pyspark.sql import SparkSession, DataFrame
from itm_data_ingestion.infrastructure.settings import settings
import re



account_name = re.search(r"AccountName=([^;]+)", settings.azure_storage_connection_string).group(1)

def make_uri(filename: str) -> str:
    """
    Construit l'URI abfss:// ou wasbs:// vers le conteneur.
    """
    return (
        f"wasbs://{settings.azure_container}"
        f"@{account_name}.blob.core.windows.net/"
        f"{filename}"
    )


def read(filename: str, spark: SparkSession) -> DataFrame:
    return (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .option("mode", "PERMISSIVE")
        .option("nullValue", "")
        .csv(make_uri(filename))
    )