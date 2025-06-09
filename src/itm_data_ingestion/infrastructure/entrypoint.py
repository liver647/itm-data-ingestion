"""A module containing the main entrypoint of the project."""


from itm_data_ingestion.utils import local_spark_session
from itm_data_ingestion.infrastructure import (
    settings,
    azure_blob_reader,
    delta_writer
)

if __name__ == "__main__":

     # Start the Spark session
    session = local_spark_session.get_spark_session(
        config=settings.settings,
    )


    # Read data from Azure
    df_clients = azure_blob_reader.read("clients.csv", session)
    df_stores       = azure_blob_reader.read("stores.csv", session)
    df_products     = azure_blob_reader.read("products.csv", session)
    df_transactions = azure_blob_reader.read("transactions_*.csv", session)

    print(f"####==================###### {df_transactions.count()} ########")
    df_clients.printSchema()
    df_stores.printSchema()
    df_products.printSchema()
    df_transactions.printSchema()