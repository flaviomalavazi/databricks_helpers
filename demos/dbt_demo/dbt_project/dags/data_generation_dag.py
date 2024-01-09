from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.utils.dates import days_ago

JOB_ID = 990107383964032

default_args = {"owner": "airflow"}

notebook_params = {
    "temporary_gcs_bucket_bq_writer": "flavio_malavazi_bucket_test",
    "target_bq_table_web_events": "flavio_malavazi.tab_analytics_events",
    "target_catalog": "flavio_malavazi",
    "target_external_location": "flavio_malavazi_ext_loc",
    "target_schema": "dbt_credit_cards_demo_raw",
    "reset_data": "false",
    "lakehouse_federation_bigquery_catalog": "lakehouse_federation_bigquery",
    "number_of_users": 5000,
    "sessions_per_day": 100,
    "simulation_duration": 120,
}

with DAG(
    "databricks_data_generation_dag",
    start_date=days_ago(2),
    schedule_interval=None,
    default_args=default_args,
) as dag:

    opr_run_now = DatabricksRunNowOperator(
        task_id="run_now",
        databricks_conn_id="databricks_default",
        job_id=JOB_ID,
        notebook_params=notebook_params,
    )
