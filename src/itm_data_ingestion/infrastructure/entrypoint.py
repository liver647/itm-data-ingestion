"""A module containing the main entrypoint of the project."""


from itm_data_ingestion.utils import local_spark_session
from itm_data_ingestion.infrastructure import settings, azure_blob_reader

from itm_data_ingestion.infrastructure.delta_writer import DeltaWriter
from itm_data_ingestion.entities.transformers import Transformers

if __name__ == "__main__":

     # Init spark session & writer
    session = local_spark_session.get_spark_session(
        config=settings.settings,
    )
    writer = DeltaWriter(settings.settings)
    transformers = Transformers(settings.settings)

    # Read new client data from ABS
    input_clients_df = azure_blob_reader.read("clients.csv", session)
    input_stores_df       = azure_blob_reader.read("stores.csv", session)
    #input_products_df     = azure_blob_reader.read("products.csv", session)
    #input_transactions_df = azure_blob_reader.read("transactions_*.csv", session)

    #input_stores_df.printSchema()
    #input_stores_df.show(truncate=False)

    # perform needed transformations
    output_stores_df = transformers.handle_stores(input_stores=input_stores_df)

    # Write transformed data to delta lake
    writer.write("clients", input_clients_df)
    writer.write("stores", output_stores_df)

    