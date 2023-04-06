"""
Test DAG to check connection to an AWS S3 bucket

Adapted from:
https://github.com/apache/airflow/blob/providers-amazon/7.3.0/tests/system/providers/amazon/aws/example_s3.py

This DAG assumes
- AWS credentials are setup either using the aws_default Airflow connection
or other methods described here
https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/connections/aws.html
- Create a new staging bucket in S3
- The AWS credentials used has the correct permissions to write to the staging bucket
- Add the name of the new staging bucket as an Airflow variable staging_bucket_name
"""

from datetime import datetime
from os import path

from airflow.models import Variable
from airflow.models.dag import DAG

from airflow.providers.amazon.aws.operators.s3 import (
    S3CreateObjectOperator,
    S3ListOperator,
)

DATA = """
    item_001,12.5
    item_002,22.5
    item_003,10.0
"""

# Empty string prefix refers to the bucket root
# See what prefix is here https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-prefixes.html
PREFIX = ""
DELIMITER = "/"


with DAG(
    "aws_s3_test",
    description="Test DAG to connect to AWS S3",
    schedule_interval="@once",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["example"],
) as dag:

    bucket_name = Variable.get("staging_bucket_name")

    prefix_name = "test_folder"
    timestamp_str = datetime.strftime(datetime.now(), "%Y-%m-%d_%H-%M-%S")
    file_name = f"test_{timestamp_str}.txt"

    # note this assumes linux type slashes, maybe not work for windows
    key = path.join(prefix_name, file_name)

    # create a text file
    task_create_object = S3CreateObjectOperator(
        task_id="create_object",
        s3_bucket=bucket_name,
        s3_key=key,
        data=DATA,
        replace=True,
    )

    # list the contents in the folder
    # check the output results in XComs
    task_list_keys = S3ListOperator(
        task_id="list_keys",
        bucket=bucket_name,
        prefix=prefix_name,
    )

    task_create_object >> task_list_keys
