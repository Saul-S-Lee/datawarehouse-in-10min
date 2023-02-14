from airflow.providers.postgres.operators.postgres import PostgresHook


def execute_query_postgres(
    conn_id, query_str,
    print_results=False,
    return_results=False,
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

    # execute the query
    cursor.execute(query_str)

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
