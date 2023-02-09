"""
Test DAG to load data into postgres
"""
import datetime as dt
from textwrap import dedent

from airflow.providers.postgres.operators.postgres import PostgresOperator

from utils.connectors import execute_query_postgres
from utils.db_check import column_list, db_check_table

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.python import PythonOperator

# [END import_module]

postgres_conn_id = "postgres_dwh"

table_name = "db_check"
temp_table = db_check_table("airflow_temp", table_name, column_list)


# [START instantiate_dag]
with DAG(
    'etl_db_check',
    default_args={'retries': 2},
    description='Test DAG to connect to postgres',
    schedule_interval=None,
    start_date=dt.datetime(2023, 1, 1),
    catchup=False,
    tags=['test'],
) as dag:

    def get_sample_data():

        return [
            ("10102", 3874),
            ("10103", 2938),
            ("10104", 938),
        ]

    task_create_temp_schema = PostgresOperator(
        task_id="create_temp_schema",
        postgres_conn_id=postgres_conn_id,
        sql=temp_table.query_create_schema(),
    )

    task_drop_temp_table = PostgresOperator(
        task_id="drop_temp_table",
        postgres_conn_id=postgres_conn_id,
        sql=temp_table.query_drop_table(),
    )

    task_create_temp_table = PostgresOperator(
        task_id="create_temp_table",
        postgres_conn_id=postgres_conn_id,
        sql=temp_table.query_create_table(),
    )

    task_count_rows_before = PostgresOperator(
        task_id="count_rows_before",
        postgres_conn_id=postgres_conn_id,
        sql=temp_table.query_count_rows(),
    )

    task_insert_data = PostgresOperator(
        task_id="insert_data_to_temp_table",
        postgres_conn_id=postgres_conn_id,
        sql=temp_table.query_insert_from_list(get_sample_data()),
    )

    # task_count_rows_after = PostgresOperator(
    #     task_id="count_rows_after",
    #     postgres_conn_id=postgres_conn_id,
    #     sql=temp_table.query_count_rows(),
    # )

    task_count_rows_after = PythonOperator(
        task_id="count_rows_after",
        python_callable=execute_query_postgres,
        op_kwargs={
            "conn_id": postgres_conn_id,
            "query_str": temp_table.query_count_rows(),
            "print_results": True,
        }
    )

    (
        task_create_temp_schema
        >> task_drop_temp_table
        >> task_create_temp_table
        >> task_count_rows_before
        >> task_insert_data
        >> task_count_rows_after
    )