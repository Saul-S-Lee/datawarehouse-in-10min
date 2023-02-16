from utils.postgres_table import column_def, postgres_table

# define the columns from the source data
source_column_top_stories = [
    column_def("event_timestamp", "TIMESTAMP"),
    column_def("event_id", "VARCHAR(36)"),
    column_def("top_stories", "VARCHAR(10000)"),
    column_def("post_processed_timestamp", "TIMESTAMP"),
]

# define any additional columns from preprocessing
processed_column_top_stories = []

column_top_stories = source_column_top_stories + processed_column_top_stories


class hacker_news_top_stories_table(postgres_table):
    """
    Table for hacker news top stories
    """
