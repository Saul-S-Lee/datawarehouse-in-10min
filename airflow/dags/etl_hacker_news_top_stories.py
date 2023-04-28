"""
DAG to extract the top stories ids from Hacker News load data into
a postgres database

This DAG is uses intermediate staging storage on S3 between
tasks.
"""
import datetime as dt
import os

from airflow.providers.postgres.operators.postgres import PostgresOperator

from utils.connectors import (
    aws_helper,
    get_json_from_api,
    execute_query_postgres,
    load_from_s3_to_postgres,
    run_preprocess,
)
from utils.hackernews import (
    column_top_stories, hacker_news_top_stories_table, parse_top_stories
)

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator, ShortCircuitOperator

with DAG(
    'etl_hacker_news_top_stories',
    default_args={'retries': 2},
    description='Extract and store Hacker News Top Stories',
    schedule_interval="5 */1 * * *",
    start_date=dt.datetime(2023, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:

    # define variables
    aws_conn_id = "aws_default"
    postgres_conn_id = "postgres_dwh"

    table_name = "top_stories"
    temp_table = hacker_news_top_stories_table(
        "airflow_temp", table_name, column_top_stories
    )
    prod_table = hacker_news_top_stories_table(
        "hacker_news", table_name, column_top_stories
    )

    timestamp_str = dt.datetime.strftime(
        dt.datetime.now(), "%Y-%m-%d_%H-%M-%S"
    )

    # set up file paths for raw and preprocessed files
    raw_bucket_name = Variable.get("staging_bucket_name")
    raw_prefix_name = "hacker_news/raw"
    raw_file_name = f"raw_{timestamp_str}.csv"
    raw_s3_key = os.path.join(raw_prefix_name, raw_file_name)

    preprocessed_bucket_name = Variable.get("staging_bucket_name")
    preprocessed_prefix_name = "hacker_news/preprocessed"
    preprocessed_file_name = f"preprocessed_{timestamp_str}.csv"
    preprocessed_s3_key = os.path.join(
        preprocessed_prefix_name, preprocessed_file_name
    )

    # define functions that will be used by tasks in the DAG
    def extract_from_api(conn_id, dest_s3_bucket_name, dest_s3_key):
        """
        Extracts the top story ids by querying the hacker news API
        Saves the ids as csv files on S3
        """
        import pandas as pd

        aws_client = aws_helper(conn_id)

        # api instructions from https://github.com/HackerNews/API
        url = 'https://hacker-news.firebaseio.com/v0/topstories.json'

        top_stories_list = get_json_from_api(url)

        if top_stories_list:
            # generate current timestamp
            date_str = dt.datetime.strftime(
                dt.datetime.now(), '%Y-%m-%d %H:%M:%S'
            )

            # parse the data into a records format
            data_list = parse_top_stories(top_stories_list, date_str)

            df = pd.DataFrame(
                data_list,
                columns=[d.get_column_name() for d in column_top_stories],
            )

            aws_client.write_csv_to_s3(
                df,
                dest_s3_bucket_name,
                dest_s3_key,
            )

            return dest_s3_key
        else:
            return None

    def preprocess(df):
        return df

    def check_file(file):
        if file:
            return True
        else:
            return False

    task_extract_from_source = PythonOperator(
        task_id="extract_from_source",
        python_callable=extract_from_api,
        op_kwargs={
            "conn_id": aws_conn_id,
            "dest_s3_bucket_name": raw_bucket_name,
            "dest_s3_key": raw_s3_key,
        }
    )

    task_check_file_exist = ShortCircuitOperator(
        task_id="check_file_exist",
        python_callable=check_file,
        op_kwargs={
            "file": "{{ti.xcom_pull(task_ids='extract_from_source')}}",
        }
    )

    task_preprocess = PythonOperator(
        task_id="preprocess",
        python_callable=run_preprocess,
        op_kwargs={
            "conn_id": aws_conn_id,
            "source_s3_bucket_name": raw_bucket_name,
            "source_s3_key": "{{ti.xcom_pull(task_ids='extract_from_source')}}",
            "dest_s3_bucket_name": preprocessed_bucket_name,
            "dest_s3_key": preprocessed_s3_key,
            "columns": [d.get_column_name() for d in column_top_stories],
            "preprocess_fun": preprocess,
            "preprocess_kwargs": None,
        }
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

    task_load_data = PythonOperator(
        task_id="load_data_to_temp_table",
        python_callable=load_from_s3_to_postgres,
        op_kwargs={
            "aws_conn_id": aws_conn_id,
            "source_s3_bucket_name": preprocessed_bucket_name,
            "source_s3_key": "{{ti.xcom_pull(task_ids='preprocess')}}",
            "postgres_conn_id": postgres_conn_id,
            "query_insert_template": temp_table.query_insert_template(),
        }
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
        task_extract_from_source
        >> task_check_file_exist
        >> task_preprocess
        >> task_create_temp_schema
        >> task_drop_temp_table
        >> task_create_temp_table
        >> (task_load_data, task_create_prod_schema)
    )
    (
        task_create_prod_schema
        >> task_create_prod_table
        >> task_count_rows_before
    )
    (
        (task_load_data, task_count_rows_before)
        >> task_load_updated_data
        >> task_count_rows_after
        >> task_inspect_data
    )
