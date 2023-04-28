from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.operators.postgres import PostgresHook


def execute_query_postgres(
    conn_id,
    query_str,
    query_val=None,
    print_results=False,
    return_results=False,
    executemany=False,
):
    """
    Executes sql query to a Postgres database. If the query generates
    results, will get the result in to a pandas dataframe
    """

    import pandas as pd

    postgres = PostgresHook(postgres_conn_id=conn_id)
    conn = postgres.get_conn()
    cursor = conn.cursor()

    print(query_str)

    # execute the query, if executemany=true, the executemany method will
    # be used, and query_val is expected to be a python list of
    # tuples of row data
    if executemany:
        print("Using cursor.executemany()")
        cursor.executemany(query_str, query_val)
    else:
        cursor.execute(query_str, query_val)

    # get results if available
    if cursor.description:
        print("Getting query results...")
        # get column names
        col_names = [desc[0] for desc in cursor.description]

        # get data
        results = cursor.fetchall()

        # generate dataframe
        results = pd.DataFrame(results, columns=col_names)
    else:
        print("Query did not return results. Nothing to fetch")
        results = None

    # commit changes
    conn.commit()

    # close connections
    cursor.close()
    conn.close()

    if print_results:
        print(results)

    if return_results:
        return results


def execute_read_sql_postgres(
    conn_id, query_str,
    print_results=False,
    return_results=False,
    pandas_kwargs=None
):
    """
    Executes sql read query to a Postgres database using pandas
    read_sql method.
    """

    import pandas as pd

    postgres = PostgresHook(postgres_conn_id=conn_id)
    conn = postgres.get_conn()

    print(query_str)

    results = pd.read_sql(query_str, conn, **(pandas_kwargs or {}))

    if print_results:
        print(results)

    if return_results:
        return results


def get_json_from_api(url):
    """
    Executes an api request and returns the json result
    """

    import requests

    r = requests.get(url)

    if r.status_code == 200:
        return r.json()
    else:
        print("error fetching results from API")
        return None


class aws_helper():

    def __init__(self, conn_id="aws_default"):
        self.conn_id = conn_id

    def get_obj_from_s3(self, s3_bucket_name, s3_key):
        from io import StringIO

        s3 = S3Hook(self.conn_id)

        print(s3_bucket_name)
        print(s3_key)

        # get object
        buffer = s3.read_key(key=s3_key, bucket_name=s3_bucket_name)

        # convert to a StringIO object
        buffer_obj = StringIO(buffer)

        return buffer_obj

    def write_csv_to_s3(self, df, s3_bucket_name, s3_key, encoding="UTF-8"):
        from io import BytesIO

        s3 = S3Hook(self.conn_id)

        # write to BytesIO object to be uploaded by boto3
        buffer = BytesIO(
            df.to_csv(index=False).encode(encoding)
        )

        s3.load_file_obj(buffer, key=s3_key, bucket_name=s3_bucket_name)

    def read_csv_froms_s3(self, s3_bucket_name, s3_key):
        import pandas as pd

        # write to BytesIO object to be uploaded by boto3
        file_obj = self.get_obj_from_s3(s3_bucket_name, s3_key)

        if file_obj:
            df = pd.read_csv(file_obj)

        return df


def run_preprocess(
    conn_id,
    source_s3_bucket_name,
    source_s3_key,
    dest_s3_bucket_name,
    dest_s3_key,
    columns,
    preprocess_fun,
    preprocess_kwargs=None,
):

    aws_client = aws_helper(conn_id)

    # retrieve the source file
    df = aws_client.read_csv_froms_s3(source_s3_bucket_name, source_s3_key)

    # run preprocessing
    df = preprocess_fun(df, **preprocess_kwargs or {})

    # validate the columns and order
    df = validate_columns(df, columns)

    # write preprocessed output to destination
    aws_client.write_csv_to_s3(df, dest_s3_bucket_name, dest_s3_key)

    return dest_s3_key


def validate_columns(df, columns):

    # fill in missing columns
    df = fill_missing_columns(df, columns)

    # get required columns in order
    df = df[columns]

    return df


def fill_missing_columns(df, columns):

    # check if the column exists in the dataframe, if not 
    # add the column and fill with empty values
    for cur_col in columns:
        if cur_col not in df.columns:
            print(f"{cur_col} not in the dataset, adding placeholder column")
            df[cur_col] = ""

    return df


def load_from_s3_to_postgres(
    aws_conn_id,
    source_s3_bucket_name,
    source_s3_key,
    postgres_conn_id,
    query_insert_template,
):
    """
    Loads data from a csv to a postgres database table
    """

    import numpy as np

    aws_client = aws_helper(aws_conn_id)

    # retrieve the source file
    df = aws_client.read_csv_froms_s3(source_s3_bucket_name, source_s3_key)

    # convert the np.nan values to None
    df.replace({np.NaN: None}, inplace=True)

    # convert dataframe to list of tuples
    data_list = df.to_records(index=False).tolist()

    # execute query
    execute_query_postgres(
        postgres_conn_id,
        query_insert_template,
        data_list,
        executemany=True,
    )
