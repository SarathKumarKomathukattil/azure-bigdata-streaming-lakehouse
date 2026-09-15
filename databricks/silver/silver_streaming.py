from pyspark import pipelines as dp
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    DoubleType,
    TimestampType
)


# ---------------------------------------------------------
# Schema for incoming ride events
# ---------------------------------------------------------

rides_schema = StructType([

    # Ride identifiers
    StructField("ride_id", StringType(), True),
    StructField("confirmation_number", StringType(), True),

    # Entity IDs
    StructField("passenger_id", StringType(), True),
    StructField("driver_id", StringType(), True),
    StructField("vehicle_id", StringType(), True),
    StructField("pickup_location_id", StringType(), True),
    StructField("dropoff_location_id", StringType(), True),

    StructField("vehicle_type_id", LongType(), True),
    StructField("vehicle_make_id", LongType(), True),
    StructField("payment_method_id", LongType(), True),
    StructField("ride_status_id", LongType(), True),
    StructField("pickup_city_id", LongType(), True),
    StructField("dropoff_city_id", LongType(), True),
    StructField("cancellation_reason_id", LongType(), True),

    # Passenger details
    StructField("passenger_name", StringType(), True),
    StructField("passenger_email", StringType(), True),
    StructField("passenger_phone", StringType(), True),

    # Driver details
    StructField("driver_name", StringType(), True),
    StructField("driver_rating", DoubleType(), True),
    StructField("driver_phone", StringType(), True),
    StructField("driver_license", StringType(), True),

    # Vehicle details
    StructField("vehicle_model", StringType(), True),
    StructField("vehicle_color", StringType(), True),
    StructField("license_plate", StringType(), True),

    # Pickup location
    StructField("pickup_address", StringType(), True),
    StructField("pickup_latitude", DoubleType(), True),
    StructField("pickup_longitude", DoubleType(), True),

    # Dropoff location
    StructField("dropoff_address", StringType(), True),
    StructField("dropoff_latitude", DoubleType(), True),
    StructField("dropoff_longitude", DoubleType(), True),

    # Ride metrics
    StructField("distance_miles", DoubleType(), True),
    StructField("duration_minutes", LongType(), True),

    # Timestamps
    StructField("booking_timestamp", TimestampType(), True),
    StructField("pickup_timestamp", StringType(), True),
    StructField("dropoff_timestamp", StringType(), True),

    # Fare information
    StructField("base_fare", DoubleType(), True),
    StructField("distance_fare", DoubleType(), True),
    StructField("time_fare", DoubleType(), True),
    StructField("surge_multiplier", DoubleType(), True),
    StructField("subtotal", DoubleType(), True),
    StructField("tip_amount", DoubleType(), True),
    StructField("total_fare", DoubleType(), True),

    # Rating
    StructField("rating", DoubleType(), True)
])


# ---------------------------------------------------------
# Create unified staging streaming table
# ---------------------------------------------------------

dp.create_streaming_table("stg_rides")


# ---------------------------------------------------------
# Historical / bulk rides
# ---------------------------------------------------------

@dp.append_flow(target="stg_rides")
def rides_bulk():

    df = spark.readStream.table("bulk_rides")

    df = df.withColumn(
        "booking_timestamp",
        col("booking_timestamp").cast("timestamp")
    )

    return df


# ---------------------------------------------------------
# Real-time ride events
# ---------------------------------------------------------

@dp.append_flow(target="stg_rides")
def rides_stream():

    df = spark.readStream.table("rides_raw")

    df_parsed = (
        df
        .withColumn(
            "parsed_rides",
            from_json(col("rides"), rides_schema)
        )
        .select("parsed_rides.*")
    )

    return df_parsed
