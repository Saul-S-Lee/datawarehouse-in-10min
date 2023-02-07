# datawarehouse-in-10min
_This repo is under construction_

__Spin up a quick data warehouse in 10 min__

The purpose of this repository is to provide an example data warehouse
that can be spun up on a local machine in 10 minutes*.

Yes, the big asterisk. *If you already have the proper pre-requistes installed and set up, the intent is that a user can clone the repo and bring up all the docker-compose services in 10 minutes (hopefully).

### Pre-requisites
- Modern computer with sufficient hardware specs (16+ GB RAM, 20+ GB free hard drive space, mid-tier multi-core processor from the past 5 years, etc.)
- Docker
- Sufficient internet connection to download the docker images

The initial phase of the development will be focused on bring in up an example Airflow pipeline to ingest data into a Postgres database. More features will be added later.