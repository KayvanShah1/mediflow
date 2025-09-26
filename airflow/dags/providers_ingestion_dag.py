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

def load_providers_to_snowflake(**context):
    hook = SnowflakeHook(snowflake_conn_id="snowflake_default")
    conn = hook.get_conn()
    cs = conn.cursor()
    try:
        cs.execute("""
        CREATE TABLE IF NOT EXISTS RAW.PROVIDERS(
            provider_id STRING,
            npi STRING,
            specialty STRING,
            state STRING
        );
        """)
        cs.execute("CREATE STAGE IF NOT EXISTS RAW.STAGE_RAW;")
        cs.execute("REMOVE @RAW.STAGE_RAW/providers/;")
        cs.execute("PUT file:///opt/airflow/data/cms_providers_sample.csv @RAW.STAGE_RAW/providers OVERWRITE=TRUE;")
        cs.execute("""
        COPY INTO RAW.PROVIDERS
        FROM @RAW.STAGE_RAW/providers
        FILE_FORMAT=(TYPE=CSV FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1)
        ON_ERROR='CONTINUE';
        """)
    finally:
        cs.close()
        conn.close()

with DAG(
    dag_id="providers_ingestion_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["mediflow", "ingestion"],
) as dag:

    load_providers = PythonOperator(
        task_id="load_providers_to_snowflake",
        python_callable=load_providers_to_snowflake,
        provide_context=True,
    )

    chain(load_providers)
