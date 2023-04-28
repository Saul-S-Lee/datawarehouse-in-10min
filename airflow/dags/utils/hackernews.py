from utils.postgres_table import column_def, postgres_table

# define the columns from the source data
source_column_top_stories = [
    column_def("event_timestamp", "TIMESTAMP"),
    column_def("event_id", "VARCHAR(36)"),
    column_def("story_id", "VARCHAR(10000)"),
    column_def("story_rank", "VARCHAR(100)"),
]

# define any additional columns from preprocessing
processed_column_top_stories = []

column_top_stories = source_column_top_stories + processed_column_top_stories


def parse_top_stories(top_stories_list, date_str):
    import uuid

    # generate a random unique for this entry
    event_id = uuid.uuid4()

    data_list = []
    for cur_i, cur_story_id in enumerate(top_stories_list):
        data_list.append(
            (date_str, event_id, str(cur_story_id), cur_i+1)
        )

    return data_list


class hacker_news_top_stories_table(postgres_table):
    """
    Table for hacker news top stories
    """

    def query_unique_stories(self):
        """
        Generate query to extract a distinct list of story ids
        """

        return f"""
        WITH ordered_tbl AS (
            SELECT
                story_id,
                event_timestamp,
                ROW_NUMBER() OVER (PARTITION BY story_id ORDER BY etl_time DESC)
                AS rn
            FROM {self.full_table_name}
        )
        SELECT
            story_id,
            event_timestamp
        FROM ordered_tbl
        WHERE
            rn = 1
        """


# define the columns from the source data
source_column_stories_catalog = [
    column_def("event_timestamp", "TIMESTAMP"),
    column_def("story_id", "VARCHAR(10000)"),
]

# define any additional columns from preprocessing
processed_column_stories_catalog = [
    column_def("post_processing_timestamp", "TIMESTAMP"),
]

column_stories_catalog = source_column_stories_catalog \
    + processed_column_stories_catalog


class hacker_news_stories_catalog_table(postgres_table):
    """
    Catalog for hacker news stories
    """

    def __init__(
        self,
        schema_name: str,
        table_name: str,
        column_list: list,
        checkpoint_id_col: str = "story_id",
    ):
        super().__init__(schema_name, table_name, column_list)
        self.checkpoint_id_col = checkpoint_id_col
