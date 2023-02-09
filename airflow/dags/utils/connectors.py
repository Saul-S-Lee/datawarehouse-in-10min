from airflow.providers.postgres.operators.postgres import PostgresHook


def execute_query_postgres(
    conn_id, query_str,
    print_results=False,
    return_results=False,
    pandas_kwargs=None
):
    import pandas as pd

    postgres = PostgresHook(postgres_conn_id=conn_id)
    conn = postgres.get_conn()

    results = pd.read_sql(query_str, conn, **(pandas_kwargs or {}))

    if print_results:
        print(results)

    if return_results:
        return results
