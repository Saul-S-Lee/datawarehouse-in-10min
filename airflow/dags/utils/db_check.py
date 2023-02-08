from utils.postgres_table import column_def, postgres_table

# define the columns from the source data
source_column_list = [
    column_def("event_id", "VARCHAR(100)"),
    column_def("val1", "VARCHAR(100)"),
]

# define any additional columns from preprocessing
processed_column_list = []

column_list = source_column_list + processed_column_list


class db_check_table(postgres_table):
    """
    Table for checking connection to the database
    """

    def __init__(self, schema_name: str, table_name: str, column_list: list):
        super().__init__(schema_name, table_name, column_list)
