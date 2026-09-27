from datetime import datetime
import os

import psycopg2

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator


def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("PGHOST", "postgres"),
        port=5432,
        database=os.getenv("PGDATABASE", "analytics_db"),
        user=os.getenv("PGUSER"),
        password=os.getenv("PGPASSWORD"),
    )


def extract_customers():
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        "SELECT id, name, email, country FROM customers"
    )

    customers = cursor.fetchall()

    cursor.close()
    conn.close()

    print(f"Extracted {len(customers)} customers")

    return customers


def validate_customers(**context):
    customers = context["ti"].xcom_pull(
        task_ids="extract_customers"
    )

    if not customers:
        raise ValueError("No customers found")

    for customer in customers:
        customer_id, name, email, country = customer

        if not name:
            raise ValueError(f"Customer {customer_id} has no name")

        if not email:
            raise ValueError(f"Customer {customer_id} has no email")

    print(f"Validated {len(customers)} customers")

    return customers


def transform_and_load(**context):
    customers = context["ti"].xcom_pull(
        task_ids="validate_customers"
    )

    conn = get_db_connection()
    cursor = conn.cursor()

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
    tags=["sre", "postgres", "etl", "xcom"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract_customers",
        python_callable=extract_customers,
    )

    validate_task = PythonOperator(
        task_id="validate_customers",
        python_callable=validate_customers,
    )

    transform_load_task = PythonOperator(
        task_id="transform_and_load",
        python_callable=transform_and_load,
    )

    extract_task >> validate_task >> transform_load_task