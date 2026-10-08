from datetime import datetime
from airflow.decorators import dag, task
from airflow.providers.standard.operators.bash import BashOperator

@dag(
    dag_id="etl_operators_demo",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["demo"],
)
def etl_operators_demo():

    @task.python
    def start():
        print("pipeline started")

    @task.bash
    def download():
        return "echo downloading file"

    process = BashOperator(
        task_id="process",
        bash_command="echo processing file",
    )

    @task.python
    def finish():
        print("pipeline finished")

    start_task = start()
    download_task = download()
    finish_task = finish()

    start_task >> download_task
    process.set_upstream(download_task)
    finish_task.set_upstream(process)

etl_operators_demo()
