from pyspark import pipelines as dp


# =========================================================
# PASSENGER DIMENSION - SCD TYPE 1
# =========================================================

@dp.view
def dim_passenger_view():

    return (
        spark.readStream.table("silver_obt")
        .select(
            "passenger_id",
            "passenger_name",
            "passenger_email",
            "passenger_phone"
        )
        .dropDuplicates(["passenger_id"])
    )


dp.create_streaming_table("dim_passenger")

dp.create_auto_cdc_flow(
    target="dim_passenger",
    source="dim_passenger_view",
    keys=["passenger_id"],
    sequence_by="passenger_id",
    stored_as_scd_type=1
)


# =========================================================
# DRIVER DIMENSION - SCD TYPE 1
# =========================================================

@dp.view
def dim_driver_view():

    return (
        spark.readStream.table("silver_obt")
        .select(
            "driver_id",
            "driver_name",
            "driver_rating",
            "driver_phone",
            "driver_license"
        )
        .dropDuplicates(["driver_id"])
    )


dp.create_streaming_table("dim_driver")

dp.create_auto_cdc_flow(
    target="dim_driver",
    source="dim_driver_view",
    keys=["driver_id"],
    sequence_by="driver_id",
    stored_as_scd_type=1
)


# =========================================================
# VEHICLE DIMENSION - SCD TYPE 1
# =========================================================

@dp.view
def dim_vehicle_view():

    return (
        spark.readStream.table("silver_obt")
        .select(
            "vehicle_id",
            "vehicle_make_id",
            "vehicle_type_id",
            "vehicle_model",
            "vehicle_color",
            "license_plate",
            "vehicle_make",
            "vehicle_type"
        )
        .dropDuplicates(["vehicle_id"])
    )


dp.create_streaming_table("dim_vehicle")

dp.create_auto_cdc_flow(
    target="dim_vehicle",
    source="dim_vehicle_view",
    keys=["vehicle_id"],
    sequence_by="vehicle_id",
    stored_as_scd_type=1
)


# =========================================================
# PAYMENT DIMENSION - SCD TYPE 1
# =========================================================

@dp.view
def dim_payment_view():

    return (
        spark.readStream.table("silver_obt")
        .select(
            "payment_method_id",
            "payment_method",
            "is_card",
            "requires_auth"
        )
        .dropDuplicates(["payment_method_id"])
    )


dp.create_streaming_table("dim_payment")

dp.create_auto_cdc_flow(
    target="dim_payment",
    source="dim_payment_view",
    keys=["payment_method_id"],
    sequence_by="payment_method_id",
    stored_as_scd_type=1
)


# =========================================================
# BOOKING DIMENSION - SCD TYPE 1
# =========================================================

@dp.view
def dim_booking_view():

    return (
        spark.readStream.table("silver_obt")
        .select(
            "ride_id",
            "confirmation_number",
            "pickup_location_id",
            "dropoff_location_id",
            "ride_status_id",
            "pickup_city_id",
            "dropoff_city_id",
            "cancellation_reason_id",
            "booking_timestamp",
            "pickup_timestamp",
            "dropoff_timestamp",
            "ride_status",
            "cancellation_reason"
        )
        .dropDuplicates(["ride_id"])
    )


dp.create_streaming_table("dim_booking")

dp.create_auto_cdc_flow(
    target="dim_booking",
    source="dim_booking_view",
    keys=["ride_id"],
    sequence_by="ride_id",
    stored_as_scd_type=1
)


# =========================================================
# LOCATION DIMENSION - SCD TYPE 2
# =========================================================

@dp.table
def dim_location_view():

    return (
        spark.readStream.table("silver_obt")
        .select(
            "pickup_city_id",
            "city",
            "state",
            "region",
            "city_updated_at"
        )
        .dropDuplicates(
            ["pickup_city_id", "city_updated_at"]
        )
    )


dp.create_streaming_table("dim_location")

dp.create_auto_cdc_flow(
    target="dim_location",
    source="dim_location_view",
    keys=["pickup_city_id"],
    sequence_by="city_updated_at",
    stored_as_scd_type=2
)


# =========================================================
# FACT TABLE - SCD TYPE 1
# =========================================================

@dp.view
def fact_view():

    return (
        spark.readStream.table("silver_obt")
        .select(
            "ride_id",
            "pickup_city_id",
            "payment_method_id",
            "driver_id",
            "passenger_id",
            "vehicle_id",

            "distance_miles",
            "duration_minutes",

            "base_fare",
            "distance_fare",
            "time_fare",
            "surge_multiplier",
            "total_fare",
            "tip_amount",
            "rating",

            "base_rate",
            "per_mile",
            "per_minute"
        )
        .dropDuplicates(["ride_id"])
    )


dp.create_streaming_table("fact")

dp.create_auto_cdc_flow(
    target="fact",
    source="fact_view",
    keys=[
        "ride_id",
        "pickup_city_id",
        "payment_method_id",
        "driver_id",
        "passenger_id",
        "vehicle_id"
    ],
    sequence_by="ride_id",
    stored_as_scd_type=1
)
