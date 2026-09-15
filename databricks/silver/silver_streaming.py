from pyspark import pipelines as dp
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import *


# Schema for incoming ride JSON
rides_schema = StructType([
    StructField("ride_id", StringType()),
    StructField("confirmation_number", StringType()),
    StructField("passenger_id", StringType()),
    StructField("driver_id", StringType()),
    StructField("vehicle_id", StringType()),
    StructField("pickup_location_id", StringType()),
    StructField("dropoff_location_id", StringType()),
    StructField("vehicle_type_id", StringType()),
    StructField("vehicle_make_id", StringType()),
    StructField("payment_method_id", StringType()),
    StructField("ride_status_id", StringType()),
    StructField("pickup_city_id", StringType()),
    StructField("dropoff_city_id", StringType()),
    StructField("cancellation_reason_id", StringType()),
    StructField("passenger_name", StringType()),
    StructField("passenger_email", StringType()),
    StructField("passenger_phone", StringType()),
    StructField("driver_name", StringType()),
    StructField("driver_phone", StringType()),
    StructField("vehicle_model", StringType()),
    StructField("vehicle_color", StringType()),
    StructField("license_plate", StringType()),
    StructField("pickup_address", StringType()),
    StructField("dropoff_address", StringType()),
    StructField("pickup_latitude", DoubleType()),
    StructField("pickup_longitude", DoubleType()),
    StructField("dropoff_latitude", DoubleType()),
    StructField("dropoff_longitude", DoubleType()),
    StructField("distance_miles", DoubleType()),
    StructField("duration_minutes", DoubleType()),
    StructField("booking_timestamp", StringType()),
    StructField("pickup_timestamp", StringType()),
    StructField("dropoff_timestamp", StringType()),
    StructField("base_fare", DoubleType()),
    StructField("distance_fare", DoubleType()),
    StructField("time_fare", DoubleType()),
    StructField("surge_multiplier", DoubleType()),
    StructField("total_fare", DoubleType()),
    StructField("tip_amount", DoubleType()),
    StructField("rating", DoubleType())
])


# Create the unified streaming staging table
dp.create_streaming_table("stg_rides")


@dp.append_flow(target="stg_rides")
def rides_bulk():

    df = spark.readStream.table("bulk_rides")

    df = df.withColumn(
        "booking_timestamp",
        col("booking_timestamp").cast("timestamp")
    )

    return df


@dp.append_flow(target="stg_rides")
def rides_stream():

    df = spark.readStream.table("rides_raw")

    parsed_df = (
        df.withColumn(
            "parsed_rides",
            from_json(col("rides"), rides_schema)
        )
        .select("parsed_rides.*")
    )

    return parsed_df
