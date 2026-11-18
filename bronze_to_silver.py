from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, row_number, max as spark_max
from pyspark.sql.window import Window
from datetime import datetime, timedelta

# =====================================================
# Spark Session
# =====================================================
spark = (
    SparkSession.builder
    .appName("Bronze_To_Silver_Generic_Driver")
    .getOrCreate()
)

# =====================================================
# Helpers
# =====================================================
def read_bq(table):
    return (
        spark.read
        .format("bigquery")
        .option("table", table)
        .load()
    )

def frequency_to_minutes(freq):
    return {
        "hourly": 60,
        "daily": 1440,
        "weekly": 10080
    }.get(freq, 1440)

# =====================================================
# Load Config Tables
# =====================================================
data_config_df = read_bq("config_metadata.DataConfig").filter(col("active") == True)
schema_config_df = read_bq("config_metadata.SchemaConfig").filter(col("active") == True)
extra_config_df = read_bq("config_metadata.ExtraConfig").filter(col("active") == True)
state_df = read_bq("config_metadata.IngestionState").filter(col("layer") == "silver")

now = datetime.utcnow()

# =====================================================
# Determine Providers to Process
# =====================================================
providers_df = (
    data_config_df
    .join(state_df, "provider_id", "left")
)

providers_to_process = []

for row in providers_df.collect():
    provider_id = row["provider_id"]
    freq_minutes = frequency_to_minutes(row["ingestion_frequency"])
    last_processed_time = row["last_processed_time"]

    if last_processed_time is None:
        providers_to_process.append(provider_id)
    else:
        if now - last_processed_time >= timedelta(minutes=freq_minutes):
            providers_to_process.append(provider_id)

print("Providers to process:", providers_to_process)

# =====================================================
# Core Processing Function
# =====================================================
def process_provider(provider_id):

    print(f"\n===== Processing provider: {provider_id} =====")

    # -------------------------------
    # DataConfig
    # -------------------------------
    data_cfg = (
        data_config_df
        .filter(col("provider_id") == provider_id)
        .collect()[0]
    )

    bronze_path = data_cfg["bronze_path"]
    silver_path = data_cfg["silver_path"]
    file_type = data_cfg["file_type"]
    delimiter = data_cfg["delimiter"]
    header = data_cfg["header"]
    write_mode = data_cfg["write_mode"]

    # -------------------------------
    # IngestionState
    # -------------------------------
    state = (
        state_df
        .filter(col("provider_id") == provider_id)
        .collect()[0]
    )

    last_processed_time = state["last_processed_time"]
    lookback_minutes = state["lookback_minutes"]

    effective_start = (
        last_processed_time - timedelta(minutes=lookback_minutes)
        if last_processed_time else None
    )

    # -------------------------------
    # Read Bronze
    # -------------------------------
    reader = spark.read.option("header", header)

    if file_type == "csv":
        reader = reader.option("delimiter", delimiter)
        df = reader.csv(bronze_path)
    elif file_type == "json":
        df = reader.json(bronze_path)
    else:
        raise Exception(f"Unsupported file type: {file_type}")

    # -------------------------------
    # Apply SchemaConfig
    # -------------------------------
    for row in schema_config_df.filter(col("provider_id") == provider_id).collect():
        if row["column_name"] in df.columns:
            df = df.withColumn(
                row["column_name"],
                col(row["column_name"]).cast(row["data_type"])
            )

    # -------------------------------
    # Incremental Filter (Late Arrival Safe)
    # -------------------------------
    if effective_start:
        df = df.filter(col("InvoiceDate") > effective_start)

    # -------------------------------
    # ExtraConfig Rules
    # -------------------------------
    for row in extra_config_df.filter(col("provider_id") == provider_id).collect():

        if row["config_type"] == "filter":
            df = df.filter(expr(row["config_value"]))

        elif row["config_type"] == "transform":
            df = df.withColumn(
                row["target_column"],
                expr(row["config_value"])
            )

    # -------------------------------
    # Deduplication
    # -------------------------------
    window = Window.partitionBy("InvoiceNo").orderBy(col("InvoiceDate").desc())

    df = (
        df.withColumn("rn", row_number().over(window))
          .filter(col("rn") == 1)
          .drop("rn")
    )

    # -------------------------------
    # Write Silver
    # -------------------------------
    (
        df.write
        .mode(write_mode)
        .parquet(silver_path)
    )

    # -------------------------------
    # Update Watermark
    # -------------------------------
    max_event_time = df.select(spark_max("InvoiceDate")).collect()[0][0]

    spark.sql(f"""
        UPDATE config_metadata.IngestionState
        SET
          last_processed_time = CURRENT_TIMESTAMP(),
          max_event_time_seen = TIMESTAMP('{max_event_time}'),
          last_run_status = 'SUCCESS',
          last_run_time = CURRENT_TIMESTAMP(),
          updated_at = CURRENT_TIMESTAMP()
        WHERE provider_id = '{provider_id}'
          AND layer = 'silver'
    """)

    print(f"Provider {provider_id} processed successfully")

# =====================================================
# Execute
# =====================================================
for provider in providers_to_process:
    try:
        process_provider(provider)
    except Exception as e:
        print(f"ERROR processing {provider}: {e}")

# =====================================================
# Stop Spark
# =====================================================
spark.stop()
