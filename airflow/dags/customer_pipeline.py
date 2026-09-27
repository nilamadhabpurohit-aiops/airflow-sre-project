from datetime import datetime

import psycopg2

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator


def transform_and_load():
    conn = psycopg2.connect(
        host="postgres",
        port=5432,
        database="analytics_db",
        user="airflow",
        password="airflowpass",
    )

    cursor = conn.cursor()

    # Extract
    cursor.execute(
        "SELECT id, name, email, country FROM customers"
    )
    customers = cursor.fetchall()

    # Transform + Load
    for customer_id, name, email, country in customers:
        cursor.execute(
            """
            INSERT INTO customer_summary
                (id, name, email, country)
            VALUES
                (%s, %s, %s, %s)
            ON CONFLICT (id)
            DO UPDATE SET
                name = EXCLUDED.name,
                email = EXCLUDED.email,
                country = EXCLUDED.country,
                processed_at = CURRENT_TIMESTAMP
            """,
            (
                customer_id,
                name,
                email,
                country.upper(),
            ),
        )

    conn.commit()

    print(f"Processed {len(customers)} customers")

    cursor.close()
    conn.close()


with DAG(
    dag_id="customer_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["sre", "postgres", "etl"],
) as dag:

    transform_and_load_task = PythonOperator(
        task_id="transform_and_load",
        python_callable=transform_and_load,
    )