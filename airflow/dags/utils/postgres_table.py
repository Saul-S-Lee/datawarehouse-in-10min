class column_def():
    """
    Defines the columns for postgres database
    """

    def __init__(
        self,
        source_col_name: str,
        data_type: str,
        modified_col_name: str = None
    ):
        """
        source_col_name = name of the column as it exist from the source file
        data_type = data_type to use in the database
        modified_col_name = alternative column name. this is helpful when
            source column names are postgres keywords
        """
        self.source_col_name = source_col_name
        self.data_type = data_type
        self.modified_col_name = modified_col_name

    def get_column_str(self):

        if self.modified_col_name:
            column_name = self.modified_col_name
        else:
            column_name = self.get_formatted_col_name()

        return f"{column_name} {self.data_type}"

    def get_column_name(self):

        if self.modified_col_name:
            column_name = self.modified_col_name
        else:
            column_name = self.get_formatted_col_name()

        return f"{column_name}"

    def get_formatted_col_name(self):
        """
        Format the source column name
        """
        column_name = str.lower(self.source_col_name)

        return column_name


class postgres_table():

    def __init__(
        self,
        schema_name: str,
        table_name: str,
        column_list: list,
        checkpoint_id_col: str = "event_id",
        checkpoint_timestamp_col: str = "event_timestamp",
    ):
        self.schema_name = schema_name
        self.table_name = table_name
        self.column_list = column_list
        self.full_table_name = f"{self.schema_name}.{self.table_name}"
        self.checkpoint_id_col = checkpoint_id_col
        self.checkpoint_timestamp_col = checkpoint_timestamp_col

    def query_create_schema(self, exists_condition=True):
        """
        Generate query to create a table in the database
        """

        if exists_condition:
            exists_str = "IF NOT EXISTS"
        else:
            exists_str = ""

        return f"""
        CREATE SCHEMA {exists_str} {self.schema_name};
        """

    def query_drop_schema(self, exists_condition=True):
        """
        Generate query to drop a schema from the database
        """

        if exists_condition:
            exists_str = "IF EXISTS"
        else:
            exists_str = ""

        return f"""
        DROP SCHEMA {exists_str} {self.schema_name};
        """

    def query_create_table(self, exists_condition=True):
        """
        Generate query to create a table in the database
        """
        query_column_str = ",\n".join(
            [column_item.get_column_str() for column_item in self.column_list]
        )

        if exists_condition:
            exists_str = "IF NOT EXISTS"
        else:
            exists_str = ""

        return f"""
        CREATE TABLE {exists_str} {self.full_table_name} (
            {query_column_str},
            etl_time TIMESTAMPTZ DEFAULT (NOW() AT TIME ZONE 'utc')
        );
        """

    def query_drop_table(self, exists_condition=True):
        """
        Generate query to drop a table from the database
        """

        if exists_condition:
            exists_str = "IF EXISTS"
        else:
            exists_str = ""

        return f"""
        DROP TABLE {exists_str} {self.full_table_name};
        """

    def query_count_rows(self):
        """
        Generate query to count all rows in a table
        """

        return f"""
        SELECT COUNT(*) FROM {self.full_table_name}
        """

    def query_select_star(self, limit=100):
        """
        Generate query to select table
        """

        return f"""
        SELECT * FROM {self.full_table_name} LIMIT {limit}
        """

    def query_insert_from_list(self, data_list):
        """
        Generate query to insert data into a table in the database

        data_list is expected to be a list of records where each
        item is a tuple of a row to be inserted
        """
        column_str = ",".join(
            [column_item.get_column_name() for column_item in self.column_list]
        )

        value_str = ",\n".join(
            [f"{data_item}" for data_item in data_list]
        )

        # replace 'NULL' with just NULL
        value_str = value_str.replace("\'NULL\'", "NULL")

        return f"""
        INSERT INTO {self.full_table_name} ({column_str})
        VALUES
            {value_str}
        ;
        """

    def query_load_updated_data(self, source_table):
        """
        Generate query to insert data into a table in the database

        data_list is expected to be a list of records where each
        item is a tuple of a row to be inserted
        """
        column_str = ",".join(
            [column_item.get_column_name() for column_item in self.column_list]
        )
        column_str += ",etl_time"

        temp_column_str = ",".join(
            [f"temp.{column_item.get_column_name()}" for column_item in self.column_list]
        )
        temp_column_str += ",etl_time"

        return f"""
        INSERT INTO {self.full_table_name} ({column_str})

        WITH prod_date_tbl AS (
            SELECT
                {self.checkpoint_id_col},
                MAX({self.checkpoint_timestamp_col}) AS max_timestamp
            FROM
                {self.full_table_name}
            GROUP BY
                {self.checkpoint_id_col}
        ),
        labeled_new_tbl AS (
            SELECT
                {temp_column_str},
                CASE
                    WHEN prod.max_timestamp IS NULL
                        THEN 1
                    WHEN temp.{self.checkpoint_timestamp_col} > prod.max_timestamp
                        THEN 1
                    ELSE
                        0
                END AS is_new
            FROM
                {source_table.full_table_name} AS temp
            LEFT JOIN
                prod_date_tbl AS prod
            ON
                temp.{self.checkpoint_id_col} = prod.{self.checkpoint_id_col}

        )
        SELECT
            {column_str}
        FROM
            labeled_new_tbl
        WHERE
            is_new = 1
        ;
        """
