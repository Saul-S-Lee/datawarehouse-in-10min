from utils.postgres_table import column_def, postgres_table

# define the columns from the source data
source_column_top_stories = [
    column_def("event_timestamp", "TIMESTAMP"),
    column_def("event_id", "VARCHAR(36)"),
    column_def("top_stories", "VARCHAR(10000)"),
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
