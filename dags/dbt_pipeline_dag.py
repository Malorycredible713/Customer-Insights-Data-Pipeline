from airflow.decorators import dag
from airflow.providers.standard.operators.bash import BashOperator
from pendulum import datetime
import os

# 1. DEFINITIONS (Absolute WSL paths)
WORKSPACE_ROOT = "/mnt/c/Users/minht/Downloads/Customers_Insight_Pipeline"
DBT_PROJECT_DIR = f"{WORKSPACE_ROOT}/jaffle_shop_duckdb"
# Use the full path to the dbt executable inside your venv
DBT_BIN = f"{WORKSPACE_ROOT}/venv/bin/dbt"

@dag(
    dag_id="dbt_jaffle_shop_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
)
def dbt_jaffle_shop_dag():

    dbt_build_task = BashOperator(
        task_id="dbt_build_pipeline",
        bash_command=f"""
        set -e
        
        # Move to the project root so 'cp' works
        cd {WORKSPACE_ROOT}
        
        # Copy seeds using absolute paths
        cp {WORKSPACE_ROOT}/raw_customers.csv {DBT_PROJECT_DIR}/seeds/
        cp {WORKSPACE_ROOT}/raw_orders.csv {DBT_PROJECT_DIR}/seeds/
        cp {WORKSPACE_ROOT}/raw_payments.csv {DBT_PROJECT_DIR}/seeds/
        
        # Run dbt with absolute binary path and project dir
        {DBT_BIN} build --project-dir {DBT_PROJECT_DIR} --profiles-dir /home/triet/.dbt
        """
    )

    dbt_build_task

dbt_jaffle_shop_dag()
