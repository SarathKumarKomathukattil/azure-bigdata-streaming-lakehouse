from pyspark import pipelines as dp
from pyspark.sql.functions import col

# Azure Event Hubs configuration
EH_NAMESPACE = "ubereventsproject12"
EH_NAME = "ubertopic"

# Retrieve the Event Hubs connection string from Databricks configuration.
# Do not hardcode secrets in GitHub.
EH_CONN_STR = spark.conf.get("connection_string")

KAFKA_OPTIONS = {
    "kafka.bootstrap.servers": f"{EH_NAMESPACE}.servicebus.windows.net:9093",
    "subscribe": EH_NAME,
    "kafka.sasl.mechanism": "PLAIN",
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.jaas.config": (
        'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule '
        f'required username="$ConnectionString" password="{EH_CONN_STR}";'
    ),
    "kafka.request.timeout.ms": "10000",
    "kafka.session.timeout.ms": "10000",
    "maxOffsetsPerTrigger": "10000",
    "failOnDataLoss": "true",
    "startingOffsets": "earliest"
}


@dp.table
def rides_raw():

    df = (
        spark.readStream
        .format("kafka")
        .options(**KAFKA_OPTIONS)
        .load()
    )

    df = df.withColumn(
        "rides",
        col("value").cast("string")
    )

    return df
