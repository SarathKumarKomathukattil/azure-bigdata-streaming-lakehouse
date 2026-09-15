from pyspark import pipelines as dp
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType
)


# ---------------------------------------------------------
# Schema for incoming real-time ride events
# ---------------------------------------------------------

rides_schema = StructType([

    # Ride identifiers
    StructField("ride_id", StringType()),
    StructField("confirmation_number", StringType()),

    # Entity IDs
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

    # Passenger details
    StructField("passenger_name", StringType()),
    StructField("passenger_email", StringType()),
    StructField("passenger_phone", StringType()),

    # Driver details
    StructField("driver_name", StringType()),
    StructField("driver_rating", DoubleType()),
    StructField("driver_phone", StringType()),
    StructField("driver_license", StringType()),

    # Vehicle details
    StructField("vehicle_model", StringType()),
    StructField("vehicle_color", StringType()),
    StructField("license_plate", StringType()),

    # Pickup details
    StructField("pickup_address", StringType()),
    StructField("pickup_latitude", DoubleType()),
    StructField("pickup_longitude", DoubleType()),

    # Drop-off details
    StructField("dropoff_address", StringType()),
    StructField("dropoff_latitude", DoubleType()),
    StructField("dropoff_longitude", DoubleType()),

    # Ride measurements
    StructField("distance_miles", DoubleType()),
    StructField("duration_minutes", DoubleType()),

    # Timestamps
    StructField("booking_timestamp", StringType()),
    StructField("pickup_timestamp", StringType()),
    StructField("dropoff_timestamp", StringType()),

    # Fare information
    StructField("base_fare", DoubleType()),
    StructField("distance_fare", DoubleType()),
    StructField("time_fare", DoubleType()),
    StructField("surge_multiplier", DoubleType()),
    StructField("subtotal", DoubleType()),
    StructField("tip_amount", DoubleType()),
    StructField("total_fare", DoubleType()),

    # Rating
    StructField("rating", DoubleType())
])


# ---------------------------------------------------------
# Unified staging streaming table
# ---------------------------------------------------------

dp.create_streaming_table("stg_rides")


# ---------------------------------------------------------
# Historical / bulk ride ingestion
# ---------------------------------------------------------

@dp.append_flow(target="stg_rides")
def rides_bulk():

    df = spark.readStream.table("bulk_rides")

    df = (
        df
        .withColumn(
            "booking_timestamp",
            col("booking_timestamp").cast("timestamp")
        )
        .withColumn(
            "pickup_timestamp",
            col("pickup_timestamp").cast("timestamp")
        )
        .withColumn(
            "dropoff_timestamp",
            col("dropoff_timestamp").cast("timestamp")
        )
    )

    return df


# ---------------------------------------------------------
# Real-time Event Hub ride ingestion
# ---------------------------------------------------------

@dp.append_flow(target="stg_rides")
def rides_stream():

    df = spark.readStream.table("rides_raw")

    parsed_df = (
        df
        .withColumn(
            "parsed_rides",
            from_json(col("rides"), rides_schema)
        )
        .select("parsed_rides.*")
    )

    parsed_df = (
        parsed_df
        .withColumn(
            "booking_timestamp",
            col("booking_timestamp").cast("timestamp")
        )
        .withColumn(
            "pickup_timestamp",
            col("pickup_timestamp").cast("timestamp")
        )
        .withColumn(
            "dropoff_timestamp",
            col("dropoff_timestamp").cast("timestamp")
        )
    )

    return parsed_df
