# datawarehouse-in-10min

__Spin up a quick data warehouse in 10 min__

The purpose of this repository is to provide an example data warehouse
that can be spun up on a local machine in 10 minutes* (_for local development_)

Yes, the big asterisk. *If you already have the proper pre-requistes installed and set up, the intent is that a user can clone the repo and bring up all the docker-compose services in 10 minutes (hopefully).

The initial phase of the development will be focused on bring in up an example Airflow pipeline to ingest data into a Postgres database. More features to be added later.

## Prerequisites
- Modern computer with sufficient hardware specs (16+ GB RAM, 20+ GB free hard drive space, mid-tier multi-core processor from the past 5 years, etc.)
- Docker
- Python environment

## Getting Started
This repo is setup as a monorepo with each top level folder as a separate miniproject with it's own docker-compose file. The purpose of this setup is to mimick an actual data platform where each component would typically be its own service.

__Note about security:__ This repo meant for local development only and not for production. As usual, please take the appropriate security precautions before deploying to production or exposing any services to internet.

### Airflow

Set up a new Fernet key [Initial setup only]

Airflow uses the Fernet key setting to encrypt secrets such as passwords. If this is not set, then any passwords that are set will not persist and be lost the next time Airflow reboots. Please check out the Airflow documentation [here](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/security/secrets/fernet.html). For convenience, I've included the code snippet in [generate_fernet_key.py](airflow/generate_fernet_key.py).

Navigate to the `airflow/` folder. Run the script

```
python generate_fernet_key.py
```

Copy the new Fernet key output and set it as a shell environment variable

```
export AIRFLOW__CORE__FERNET_KEY=your_fernet_key
```

Run Airflow using docker compose

```
docker compose up
```

Browse to localhost:8080 to make sure Airflow has launched correctly. The default login is airflow and password is airflow.

### Postgres Database

To make the data warehouse more self contained, we are setting up a local Postgres database to store the data from the Airflow pipelines. In principle, the Airflow service can connect to any other local or cloud hosted Postgres database as long as the database is set up as a connection in Airflow.

Navigate to the `postgres/` directory

Launch the Postgres database using docker compose

```
docker compose up
```

### Setup an Airflow connection to the Postgres database

Log in to the Airflow browser UI

Click Admin > Connections

Add a new connection called `postgres_dwh` with the following settings:
- Connection Id = `postgres_dwh`
- Connection Type = `Postgres`
- Host = `host.docker.internal`
- Login = `postgres`
- Password = `password` (please see the .env file)
- Port = `15432` (feel free to set this to another port here and in the docker-compose.yaml file)

Click Save

### AWS S3 Staging Bucket (Optional)

For more complex workflows, an intermediate cloud staging location (e.g. AWS S3 or Google Cloud Storage) helps to facilitate the movement of data. This is the preferred method as Airflow XComs is not meant for data transfer. This step is marked as optional as it would most likely require 10+ minutes to set up.

The DAGs that do not require an intermediate staging bucket have "no_staging" in the name.

The example DAGs use AWS S3 as the intermediate cloud staging location. But the DAGs can be adapted to use other cloud storage services.

Setting up AWS S3 Bucket 
- AWS credentials are setup either using the aws_default
    Airflow connection or other methods described here
https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/connections/aws.html
- Create a new staging bucket in S3
- The AWS credentials used has the correct permissions to write to the     staging bucket
- Add the name of the new staging bucket as an Airflow variable
    staging_bucket_name

### Run the example pipelines
There are some example Airflow pipelines in the (airflow/dags) folder.

__etl_aws_s3_check__ <br>
Run this pipeline to check the connection with an AWS S3 bucket

__etl_db_check__ <br>
Run this pipeline to check the connection with the Postgres database


__etl_hacker_news_top_stories_no_staging__ <br>
This version does not require an AWS S3 intermediate staging bucket. Use this version for fast start up.
Run this pipeline to start extracting and logging the top story ids from [Hacker News](https://news.ycombinator.com/).

__etl_hacker_news_top_stories__ <br>
This version requires an AWS S3 intermediate staging bucket.
Run this pipeline to start extracting and logging the top story ids from [Hacker News](https://news.ycombinator.com/).
