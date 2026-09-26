from datetime import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator


def hello():
    print("Hello from KubernetesExecutor!")
    print("This task is running inside a Kubernetes Pod.")


with DAG(
    dag_id="hello_sre",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["sre", "learning"],
) as dag:

    hello_task = PythonOperator(
        task_id="hello",
        python_callable=hello,
    )
