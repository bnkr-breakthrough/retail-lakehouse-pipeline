from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="retail_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["retail", "lakehouse"]
) as dag:

    bronze = BashOperator(
        task_id="bronze_ingestion",
        bash_command="""
        export DATA_PATH=/opt/airflow/data &&
        python /opt/airflow/scripts/bronze_ingestion.py
        """
    )

    silver = BashOperator(
        task_id="silver_transformation",
        bash_command="""
        export DATA_PATH=/opt/airflow/data &&
        python /opt/airflow/scripts/silver_transformation.py
        """
    )

    gold = BashOperator(
        task_id="gold_transformation",
        bash_command="""
        export DATA_PATH=/opt/airflow/data &&
        python /opt/airflow/scripts/gold_transformation.py
        """
    )

    snowflake = BashOperator(
        task_id="load_to_snowflake",
        bash_command="""
        export DATA_PATH=/opt/airflow/data &&
        export CONFIG_PATH=/opt/airflow/config &&
        python /opt/airflow/scripts/load_to_snowflake.py
        """
    )

    bronze >> silver >> gold >> snowflake
