import pandas as pd

# Reference datasets ingested from ADLS Gen2
REFERENCE_FILES = [
    "map_cities",
    "map_cancellation_reasons",
    "map_payment_methods",
    "map_ride_statuses",
    "map_vehicle_makes",
    "map_vehicle_types"
]

# Storage location
BASE_URL = "https://datalakeuberproject12.blob.core.windows.net/raw/ingestion"

# Configure the SAS token securely in the Databricks environment.
# Never commit the real token to GitHub.
SAS_TOKEN = "<ADLS_SAS_TOKEN>"


# ---------------------------------------------------------
# Load reference datasets into Bronze Delta tables
# ---------------------------------------------------------

for filename in REFERENCE_FILES:

    url = f"{BASE_URL}/{filename}.json?{SAS_TOKEN}"

    df = pd.read_json(url)

    df_spark = spark.createDataFrame(df)

    (
        df_spark.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"uber.bronze.{filename}")
    )


# ---------------------------------------------------------
# Load historical bulk ride data
# ---------------------------------------------------------

bulk_url = f"{BASE_URL}/bulk_rides.json?{SAS_TOKEN}"

df = pd.read_json(bulk_url)

df_spark = spark.createDataFrame(df)


# Create the table only if it does not already exist
if not spark.catalog.tableExists("uber.bronze.bulk_rides"):

    (
        df_spark.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable("uber.bronze.bulk_rides")
    )

    print("bulk_rides table created successfully")
