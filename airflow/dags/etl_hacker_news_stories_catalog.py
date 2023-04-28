"""
DAG to extract and genereate the distinct top stories ids 
"""
import datetime as dt
import os

from airflow.providers.postgres.operators.postgres import PostgresOperator

from utils.connectors import (
    check_file,
    execute_query_postgres,
    load_from_s3_to_postgres,
    sql_to_s3,
    run_preprocess,
)
from utils.hackernews import (
    column_stories_catalog,
    column_top_stories,
    hacker_news_top_stories_table,
    hacker_news_stories_catalog_table,
)

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator, ShortCircuitOperator

with DAG(
    'etl_hacker_news_stories_catalog',
    default_args={'retries': 2},
    description='Generate Hacker News Stories Catalog',
    schedule_interval="10 */1 * * *",
    start_date=dt.datetime(2023, 1, 1),
    catchup=False,
    tags=['Hacker News'],
) as dag:

    # define variables
    aws_conn_id = "aws_default"
    postgres_conn_id = "postgres_dwh"

    table_name = "stories_catalog"
    temp_table = hacker_news_stories_catalog_table(
        "airflow_temp", table_name, column_stories_catalog
    )
    prod_table = hacker_news_stories_catalog_table(
        "hacker_news", table_name, column_stories_catalog
    )

    source_table_name = "top_stories"
    source_table = hacker_news_top_stories_table(
        "hacker_news", source_table_name, column_top_stories
    )

    timestamp_str = dt.datetime.strftime(
        dt.datetime.now(), "%Y-%m-%d_%H-%M-%S"
    )

    # set up file paths for raw and preprocessed files
    raw_bucket_name = Variable.get("staging_bucket_name")
    raw_prefix_name = "hacker_news/stories_catalog/raw"
    raw_file_name = f"raw_stories_catalog_{timestamp_str}.csv"
    raw_s3_key = os.path.join(raw_prefix_name, raw_file_name)

    preprocessed_bucket_name = Variable.get("staging_bucket_name")
    preprocessed_prefix_name = "hacker_news/stories_catalog/preprocessed"
    preprocessed_file_name = f"preprocessed_stories_catalog_{timestamp_str}.csv"
    preprocessed_s3_key = os.path.join(
        preprocessed_prefix_name, preprocessed_file_name
    )

    def preprocess(df):
        df["post_processing_timestamp"] = None
        return df

    task_extract_from_source = PythonOperator(
        task_id="extract_from_source",
        python_callable=sql_to_s3,
        op_kwargs={
            "aws_conn_id": aws_conn_id,
            "dest_s3_bucket_name": raw_bucket_name,
            "dest_s3_key": raw_s3_key,
            "postgres_conn_id": postgres_conn_id,
            "query_string": source_table.query_unique_stories(),
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
            "columns": [d.get_column_name() for d in column_stories_catalog],
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
