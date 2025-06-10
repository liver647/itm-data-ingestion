# ITM Data Ingestion

## Description
Repository holding the source code for handling data ingestion of new client.

## Local Setup

Please make sure you have:
- Poetry 2.1.3
- Java 11
- Spark 3.5.6

Please make also sure to define JAVA_HOME & SPARK_HOME environnement

Install the local package (requires poetry):
```shell
poetry install
```

Copy the `.env.example` file to `.env` and correct the values if needed.

## Usage

```shell
poetry run spark-submit --packages "org.apache.hadoop:hadoop-azure:3.3.4,io.delta:delta-spark_2.12:3.3.2" src/itm_data_ingestion/infrastructure/entrypoint.py
```


## Testing

To run the tests, run:
```shell
poetry run pytest
```
