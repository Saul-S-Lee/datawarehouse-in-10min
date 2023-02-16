"""
DAG to extract the top stories ids from Hacker News load data into
a postgres database

This DAG is designed to run without intermediate staging storage between
tasks. Consequently, the extract and load is done in the same python
function step
"""
import datetime as dt

from airflow.providers.postgres.operators.postgres import PostgresOperator

from utils.connectors import execute_query_postgres, get_json_from_api
from utils.hackernews import column_top_stories, hacker_news_top_stories_table

from airflow import DAG

from airflow.operators.python import PythonOperator

with DAG(
    'etl_hacker_news_top_stories',
    default_args={'retries': 2},
    description='Extract and store Hacker News Top Stories',
    schedule_interval="5 */1 * * *",
    start_date=dt.datetime(2023, 1, 1),
    catchup=False,
    tags=['no_staging_storage'],
) as dag:

    # define variables
    postgres_conn_id = "postgres_dwh"

    table_name = "top_stories"
    temp_table = hacker_news_top_stories_table("airflow_temp", table_name, column_top_stories)
    prod_table = hacker_news_top_stories_table("hacker_news", table_name, column_top_stories)

    def extract_and_load(conn_id):
        """
        Extracts the top story ids by querying the hacker news API
        Loads the data into the database
        """
        import uuid

        # api instructions from https://github.com/HackerNews/API
        url = 'https://hacker-news.firebaseio.com/v0/topstories.json'

        top_stories_list = get_json_from_api(url)

        # generate current timestamp
        date_str = dt.datetime.strftime(dt.datetime.now(), '%Y-%m-%d %H:%M:%S')

        # generate a random unique for this entry
        event_id = uuid.uuid4()

        data_list = [
            (date_str, event_id, str(top_stories_list), None)
        ]

        execute_query_postgres(
            conn_id,
            temp_table.query_insert_template(),
            data_list,
            executemany=True,
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

    task_count_rows_before = PythonOperator(
        task_id="count_rows_before",
        python_callable=execute_query_postgres,
        op_kwargs={
            "conn_id": postgres_conn_id,
            "query_str": prod_table.query_count_rows(),
            "print_results": True,
        }
    )

    task_extract_and_load_data = PythonOperator(
        task_id="extract_and_load_data",
        python_callable=extract_and_load,
        op_kwargs={"conn_id": postgres_conn_id}
    )

    task_count_rows_after = PythonOperator(
        task_id="count_rows_after",
        python_callable=execute_query_postgres,
        op_kwargs={
            "conn_id": postgres_conn_id,
            "query_str": prod_table.query_count_rows(),
            "print_results": True,
        }
    )

    task_create_prod_schema = PostgresOperator(
        task_id="create_prod_schema",
        postgres_conn_id=postgres_conn_id,
        sql=prod_table.query_create_schema(),
    )

    task_create_prod_table = PostgresOperator(
        task_id="create_prod_table",
        postgres_conn_id=postgres_conn_id,
        sql=prod_table.query_create_table(),
    )

    task_load_updated_data = PostgresOperator(
        task_id="load_updated_data",
        postgres_conn_id=postgres_conn_id,
        sql=prod_table.query_load_updated_data(temp_table),
    )

    task_inspect_data = PythonOperator(
        task_id="data_check",
        python_callable=execute_query_postgres,
        op_kwargs={
            "conn_id": postgres_conn_id,
            "query_str": prod_table.query_select_star(limit=10),
            "print_results": True,
        }
    )

    (
        task_create_temp_schema
        >> task_drop_temp_table
        >> task_create_temp_table
        >> task_extract_and_load_data
        >> task_create_prod_schema
        >> task_create_prod_table
        >> task_count_rows_before
        >> task_load_updated_data
        >> task_count_rows_after
        >> task_inspect_data
    )
