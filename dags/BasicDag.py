from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime

with DAG(
    dag_id="do_nothing_dag",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,  # Set to None for manual triggering or no periodic runs
    catchup=False,
    tags=["example", "empty"],
) as dag:
    # Define a task that does nothing
    start_task = EmptyOperator(task_id="start_task")

    # Define another task that also does nothing
    end_task = EmptyOperator(task_id="end_task")

    # Define a dependency (start_task runs before end_task)
    start_task >> end_task



