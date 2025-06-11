"""A module containing transformers tests."""

import pytest
from unittest.mock import Mock
from pyspark.sql import SparkSession, Row
from pyspark import SparkConf
from itm_data_ingestion.entities.transformers import Transformers


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    
    return (
        SparkSession.builder
        .appName("TransformersTest")
        .master("local[1]")
        .getOrCreate()
    )


@pytest.fixture
def transformers():
    mock_config = Mock()
    return Transformers(mock_config)


def test_handle_stores(spark, transformers):
    """
    test normal store data ingestion 
    """
    input_stores = [
        Row(store_id=1, latlng="(48.85,2.35)", opening="08:00", closing="20:00"),
        Row(store_id=2, latlng="(43.60,1.44)", opening="09:00", closing="21:00"),
    ]
    input_stores_df = spark.createDataFrame(input_stores)

    output_stores_df = transformers.handle_stores(input_stores_df).collect()

    assert output_stores_df[0]["latitude"] == 48.85
    assert output_stores_df[0]["longitude"] == 2.35


def test_handle_transactions(spark, transformers):
    """
    test normal transaction ingestion
    """
    transactions_data = [Row(transaction_id=1, client_id=101, date="2023-12-01", hour="14", minute="30")]
    clients_data = [Row(id=101, account_id=123)]

    transactions_df = spark.createDataFrame(transactions_data)
    clients_df = spark.createDataFrame(clients_data)

    final_transactions_df = transformers.handle_transactions(transactions_df, clients_df).collect()[0]

    assert final_transactions_df["account_id"] == 123
    assert final_transactions_df["timestamp_utc"].strftime("%Y-%m-%d %H:%M") == "2023-12-01 14:30"


def test_transaction_with_unknown_client(spark, transformers):
    """
    transaction with unknown client id should produce final transaction with null account id
    """
    transactions_data = [Row(transaction_id=1, client_id=666, date="2023-12-01", hour="14", minute="30")]
    clients_data = [Row(id=101, account_id=123)]

    transactions_df = spark.createDataFrame(transactions_data)
    clients_df = spark.createDataFrame(clients_data)

    final_transactions_df = transformers.handle_transactions(transactions_df, clients_df).collect()[0]

    assert final_transactions_df["account_id"] is None
    

def test_transaction_with_invalid_date_format(spark, transformers):
    """
    transaction with invalid date format should produce final transaction with null timestamp
    """
    transactions_data = [Row(transaction_id=1, client_id=101, date="12-2023-01", hour="14", minute="30")]
    clients_data = [Row(id=101, account_id=123)]

    transactions_df = spark.createDataFrame(transactions_data)
    clients_df = spark.createDataFrame(clients_data)

    final_transactions_df = transformers.handle_transactions(transactions_df, clients_df).collect()[0]

    assert final_transactions_df["timestamp_utc"] is None
    assert final_transactions_df["account_id"] == 123