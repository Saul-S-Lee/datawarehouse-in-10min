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

    def __init__(self, schema_name: str, table_name: str, column_list: list):
        self.schema_name = schema_name
        self.table_name = table_name
        self.column_list = column_list
        self.full_table_name = f"{self.schema_name}.{self.table_name}"

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
            {query_column_str}
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
            [str(data_item) for data_item in data_list]
        )

        return f"""
        INSERT INTO {self.full_table_name} ({column_str})
        VALUES
            {value_str}
        ;
        """
