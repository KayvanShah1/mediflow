from datetime import datetime, timedelta
from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

DEFAULT_ARGS = {
    "owner": "mediflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

def load_claims_to_snowflake(**context):
    """
    Minimal demo: loads CSV from local mount (or S3/HTTP if you adapt) into RAW.CLAIMS using Snowflake PUT/COPY.
    Assumes an Airflow Connection named 'snowflake_default' and an internal stage 'RAW.STAGE_RAW'.
    """
    hook = SnowflakeHook(snowflake_conn_id="snowflake_default")
    conn = hook.get_conn()
    cs = conn.cursor()
    try:
        # Create table if not exists (demo schema)
        cs.execute("""
        CREATE TABLE IF NOT EXISTS RAW.CLAIMS(
            claim_id STRING,
            patient_id STRING,
            provider_id STRING,
            service_date DATE,
            allowed_amount FLOAT,
            paid_amount FLOAT,
            drg_code STRING
        );
        """)
        # Create stage if not exists (internal)
        cs.execute("CREATE STAGE IF NOT EXISTS RAW.STAGE_RAW;")

        # Put file into stage (adjust local path if using Docker volume)
        # For cloud: copy from @external stage or use COPY FROM @stage (S3/GCS external stage)
        cs.execute("REMOVE @RAW.STAGE_RAW/claims/;")  # clean stage path
        cs.execute("PUT file:///opt/airflow/data/cms_claims_sample.csv @RAW.STAGE_RAW/claims OVERWRITE=TRUE;")

        # Copy into table
        cs.execute("""
        COPY INTO RAW.CLAIMS
        FROM @RAW.STAGE_RAW/claims
        FILE_FORMAT=(TYPE=CSV FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1)
        ON_ERROR='CONTINUE';
        """)
    finally:
        cs.close()
        conn.close()

with DAG(
    dag_id="claims_ingestion_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["mediflow", "ingestion"],
) as dag:

    load_claims = PythonOperator(
        task_id="load_claims_to_snowflake",
        python_callable=load_claims_to_snowflake,
        provide_context=True,
    )

    chain(load_claims)
