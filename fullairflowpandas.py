from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.utils.task_group import TaskGroup
from google.cloud import storage
from datetime import timedelta
import pandas as pd
from io import BytesIO

# =====================================================
# GLOBAL CONFIG
# =====================================================
PROJECT_ID = "your-gcp-project"
REGION = "us-central1"
CLUSTER_NAME = "retail-dataproc-cluster"
BUCKET = "retail-datalake"

# =====================================================
# DEFAULT ARGS
# =====================================================
default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}

# =====================================================
# METADATA HELPERS
# =====================================================
def get_active_providers():
    hook = BigQueryHook()
    sql = """
    SELECT DISTINCT provider_id
    FROM config_metadata.DataConfig
    WHERE active = TRUE
    """
    df = hook.get_pandas_df(sql)
    return df["provider_id"].tolist()


def get_data_config(provider):
    hook = BigQueryHook()
    sql = f"""
    SELECT raw_path
    FROM config_metadata.DataConfig
    WHERE provider_id = '{provider}'
      AND active = TRUE
    """
    df = hook.get_pandas_df(sql)
    if df.empty:
        raise ValueError(f"No DataConfig found for {provider}")
    return df.iloc[0]


def get_expected_schema(provider):
    hook = BigQueryHook()
    sql = f"""
    SELECT column_name
    FROM config_metadata.SchemaConfig
    WHERE provider_id = '{provider}'
      AND active = TRUE
    """
    df = hook.get_pandas_df(sql)
    return set(df["column_name"].tolist())

# =====================================================
# VALIDATION TASK
# =====================================================
def validate_file(provider):
    cfg = get_data_config(provider)
    raw_path = cfg["raw_path"]

    client = storage.Client()
    blobs = list(client.list_blobs(BUCKET, prefix=raw_path))

    if not blobs:
        raise ValueError(f"No files found for {provider}")

    for blob in blobs:
        if blob.size == 0:
            raise ValueError(f"Empty file detected: {blob.name}")

    # Schema validation (lightweight)
    expected_cols = get_expected_schema(provider)
    sample = blobs[0].download_as_bytes()
    df = pd.read_csv(BytesIO(sample), nrows=5)

    missing_cols = expected_cols - set(df.columns)
    if missing_cols:
        raise ValueError(f"Missing columns {missing_cols} for {provider}")

    print(f"Validation passed for provider {provider}")

# =====================================================
# DAG DEFINITION
# =====================================================
with DAG(
    dag_id="retail_metadata_driven_pipeline",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval="@daily",
    catchup=False,
    tags=["metadata", "bronze", "silver", "gold"],
) as dag:

    providers = get_active_providers()

    for provider in providers:

        with TaskGroup(group_id=f"{provider}_pipeline") as tg:

            # 1️⃣ Validate raw files
            validate_task = PythonOperator(
                task_id="validate_file",
                python_callable=validate_file,
                op_kwargs={"provider": provider},
            )

            # 2️⃣ Move to Bronze
            move_to_bronze = GCSToGCSOperator(
                task_id="move_to_bronze",
                source_bucket=BUCKET,
                source_object=f"provider-ingress/raw/{provider}/",
                destination_bucket=BUCKET,
                destination_object=f"bronze/raw/{provider}/",
                move_object=True,
            )

            # 3️⃣ Dataproc Spark Job (Bronze → Silver)
            run_dataproc = DataprocSubmitJobOperator(
                task_id="run_dataproc_job",
                project_id=PROJECT_ID,
                region=REGION,
                job={
                    "placement": {"cluster_name": CLUSTER_NAME},
                    "pyspark_job": {
                        "main_python_file_uri": "gs://retail-code/jobs/bronze_to_silver.py",
                        "args": [provider],
                    },
                },
            )

            # 4️⃣ Build Gold in BigQuery
            build_gold = BigQueryInsertJobOperator(
                task_id="run_bq_gold",
                configuration={
                    "query": {
                        "query": f"""
                        CREATE OR REPLACE TABLE gold_retail.daily_sales
                        PARTITION BY sale_date
                        AS
                        SELECT
                          provider_id,
                          DATE(InvoiceDate) AS sale_date,
                          Country,
                          SUM(Quantity * UnitPrice) AS total_sales
                        FROM silver_retail.ext_retail_sales
                        WHERE provider_id = '{provider}'
                        GROUP BY provider_id, sale_date, Country
                        """,
                        "useLegacySql": False,
                    }
                },
            )

            validate_task >> move_to_bronze >> run_dataproc >> build_gold