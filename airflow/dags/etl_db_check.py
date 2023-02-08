"""
Test DAG to load data into postgres
"""
import datetime as dt
from textwrap import dedent

from airflow.providers.postgres.operators.postgres import PostgresOperator

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

    # [START extract_function]
    def extract(**kwargs):
        ti = kwargs['ti']
        data_string = '{"1001": 301.27, "1002": 433.21, "1003": 502.22}'
        ti.xcom_push('order_data', data_string)

    # [END extract_function]

    data_to_insert = [
        ("1001", 301.27),
        ("1002", 433.21),
        ("1003", 502.22),
    ]

    # [START main_flow]
    task_extract = PythonOperator(
        task_id='extract',
        python_callable=extract,
    )
    task_extract.doc_md = dedent(
        """\
    #### Extract task
    A simple Extract task to get data ready for the rest of the data pipeline.
    In this case, getting data is simulated by reading from a hardcoded JSON string.
    This data is then put into xcom, so that it can be processed by the next task.
    """
    )

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

    task_insert_data = PostgresOperator(
        task_id="insert_data_to_temp_table",
        postgres_conn_id=postgres_conn_id,
        sql=temp_table.query_insert_from_list(data_to_insert),
    )

    (
        task_extract
        >> task_create_temp_schema
        >> task_drop_temp_table
        >> task_create_temp_table
        >> task_insert_data
    )
