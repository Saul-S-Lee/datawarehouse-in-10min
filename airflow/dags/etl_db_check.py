"""
Test DAG to check connection to a postgres database
and load data into postgres
"""
import datetime as dt

from airflow.providers.postgres.operators.postgres import PostgresOperator

from utils.connectors import execute_query_postgres
from utils.db_check import column_list, db_check_table

from airflow import DAG

from airflow.operators.python import PythonOperator

# define variables
# please setup a connection named postgres_dwh to the postgres db to be used
postgres_conn_id = "postgres_dwh"

table_name = "db_check"
temp_table = db_check_table("airflow_temp", table_name, column_list)


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
        return ("10102", 3874)

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

    # This task uses execute_query_postgres() function because
    # a returned value is required
    task_count_rows_before = PythonOperator(
        task_id="count_rows_before",
        python_callable=execute_query_postgres,
        op_kwargs={
            "conn_id": postgres_conn_id,
            "query_str": temp_table.query_count_rows(),
            "print_results": True,
        }
    )

    task_insert_data = PostgresOperator(
        task_id="insert_data_to_temp_table",
        postgres_conn_id=postgres_conn_id,
        sql=temp_table.query_insert_template(),
        parameters=get_sample_data(),
    )

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
