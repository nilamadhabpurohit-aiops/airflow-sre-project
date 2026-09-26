from datetime import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
import psycopg2


def read_customers():
    conn = psycopg2.connect(
        host="postgres",
        port=5432,
        database="analytics_db",
        user="airflow",
        password="airflowpass",
    )

    cursor = conn.cursor()

    cursor.execute("SELECT id, name, email, country FROM customers")

    customers = cursor.fetchall()

    for customer in customers:
        print(customer)

    cursor.close()
    conn.close()


with DAG(
    dag_id="customer_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["sre", "postgres", "learning"],
) as dag:

    read_customers_task = PythonOperator(
        task_id="read_customers",
        python_callable=read_customers,
    )