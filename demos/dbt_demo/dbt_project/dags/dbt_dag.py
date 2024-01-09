from airflow import DAG
from airflow.providers.databricks.operators.databricks import (
    DatabricksSubmitRunDeferrableOperator,
)
from airflow.utils.dates import days_ago

JOB_ID = 990107383964032

default_args = {"owner": "airflow"}

with DAG(
    "databricks_dbt_project_task",
    start_date=days_ago(2),
    schedule_interval=None,
    default_args=default_args,
) as dag:

    opr_run_now = DatabricksSubmitRunDeferrableOperator(
        task_id="run_now",
        databricks_conn_id="databricks_default",
        job_id=JOB_ID,
        dbt_task=notebook_params,
    )
