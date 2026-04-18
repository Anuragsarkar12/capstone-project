import yaml
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator



CONFIG_PATH = "/opt/airflow/dags/config/config.yaml"

with open(CONFIG_PATH, "r") as f:
    config = yaml.safe_load(f)

airflow_cfg = config["airflow"]



DATABRICKS_CONN_ID = airflow_cfg["databricks"]["conn_id"]
CLUSTER_ID = airflow_cfg["databricks"]["cluster_id"]
NOTEBOOK_PATHS = airflow_cfg["notebooks"]

DAG_CONFIG = airflow_cfg["dag"]



default_args = {
    "owner": DAG_CONFIG["owner"],
    "depends_on_past": False,
    "retries": DAG_CONFIG["retries"],
    "retry_delay": timedelta(minutes=DAG_CONFIG["retry_delay_minutes"]),
    "start_date": datetime.fromisoformat(DAG_CONFIG["start_date"]),
}



with DAG(
    dag_id=DAG_CONFIG["dag_id"],
    default_args=default_args,
    schedule_interval=DAG_CONFIG["schedule_interval"],
    catchup=DAG_CONFIG["catchup"],
    max_active_runs=DAG_CONFIG["max_active_runs"],
    tags=DAG_CONFIG["tags"],
) as dag:

    ingest_raw = DatabricksSubmitRunOperator(
        task_id="ingest_raw",
        databricks_conn_id=DATABRICKS_CONN_ID,
        json={
            "existing_cluster_id": CLUSTER_ID,
            "notebook_task": {
                "notebook_path": NOTEBOOK_PATHS["bronze"]
            }
        }
    )

    build_silver = DatabricksSubmitRunOperator(
        task_id="build_silver",
        databricks_conn_id=DATABRICKS_CONN_ID,
        json={
            "existing_cluster_id": CLUSTER_ID,
            "notebook_task": {
                "notebook_path": NOTEBOOK_PATHS["silver"]
            }
        }
    )

    dq_check_silver = DatabricksSubmitRunOperator(
        task_id="dq_check_silver",
        databricks_conn_id=DATABRICKS_CONN_ID,
        json={
            "existing_cluster_id": CLUSTER_ID,
            "notebook_task": {
                "notebook_path": NOTEBOOK_PATHS["dq"],
                "base_parameters": {
                    "layer": airflow_cfg["dq_layers"]["silver"]
                }
            }
        }
    )

    build_gold = DatabricksSubmitRunOperator(
        task_id="build_gold",
        databricks_conn_id=DATABRICKS_CONN_ID,
        json={
            "existing_cluster_id": CLUSTER_ID,
            "notebook_task": {
                "notebook_path": NOTEBOOK_PATHS["gold"]
            }
        }
    )

    dq_check_gold = DatabricksSubmitRunOperator(
        task_id="dq_check_gold",
        databricks_conn_id=DATABRICKS_CONN_ID,
        json={
            "existing_cluster_id": CLUSTER_ID,
            "notebook_task": {
                "notebook_path": NOTEBOOK_PATHS["dq"],
                "base_parameters": {
                    "layer": airflow_cfg["dq_layers"]["gold"]
                }
            }
        }
    )

    load_snowflake = DatabricksSubmitRunOperator(
        task_id="load_snowflake",
        databricks_conn_id=DATABRICKS_CONN_ID,
        json={
            "existing_cluster_id": CLUSTER_ID,
            "notebook_task": {
                "notebook_path": NOTEBOOK_PATHS["snowflake"]
            }
        }
    )

   

    ingest_raw >> build_silver >> dq_check_silver >> build_gold >> dq_check_gold >> load_snowflake