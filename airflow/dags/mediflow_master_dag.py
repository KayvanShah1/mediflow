from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.bash import BashOperator

DEFAULT_ARGS = {
    "owner": "mediflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="mediflow_master_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["mediflow", "orchestration"],
) as dag:

    trigger_claims = TriggerDagRunOperator(
        task_id="trigger_claims_ingestion",
        trigger_dag_id="claims_ingestion_dag",
        wait_for_completion=True,
    )

    trigger_providers = TriggerDagRunOperator(
        task_id="trigger_providers_ingestion",
        trigger_dag_id="providers_ingestion_dag",
        wait_for_completion=True,
    )

    # Example: run dbt after ingestion (expects dbt installed in the image or via volume)
    dbt_stg = BashOperator(
        task_id="dbt_run_stg",
        bash_command="cd /opt/airflow/dbt_project && dbt run --select stg",
    )
    dbt_core = BashOperator(
        task_id="dbt_run_core",
        bash_command="cd /opt/airflow/dbt_project && dbt run --select dim+,fact+",
    )
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/airflow/dbt_project && dbt test",
    )

    trigger_claims >> trigger_providers >> dbt_stg >> dbt_core >> dbt_test
