# Sparkify Database Warehouse

## Overview

This project contains the schema and ETL data pipeline code for a database warehouse for the music streaming app Sparkify.

The ETL and data warehouse has been built on AWS, with a PostgreSQL database and
staging tables hosted on Amazon Redshift, pulling data from Amazon S3. The
analytics tables have been arranged in a star schema to allow the Sparkify team
to readily run queries to analyze user activity on their app, such as on what
songs users are listening to. The scripts have been created in Python.

The platform hosting the data warehouse is AWS, specifically S3 for the source data and Redshift for the Postgres warehouse. The data warehouse is optimized for OLAP via the usage of a star schema.
Both the DDL and ETL code are executed from Python scripts.

## Structure

The project contains the following components:

```bash
├── README.md - This file.
├── create_tables.py # Python script with all methods necessary to recreate the data warehouse.
├── etl.py # Python script with all methods necessary to extract the trigger the execution of the ETL process.
├── dwh.cfg # Config file to be populated with credentials for accessing all cloud resources.
└── sql_queries.py # Python script defining the data warehouse schema and prepared/reusable queries.
```

## Database Schema

The database contains the following fact table:

* `songplays` - user song plays

`songplays` has foreign keys to the following (self-explanatory) dimension
tables:

* `users`
* `songs`
* `artists`
* `time`

## Instructions

You will need to update `dwh.cfg` with the following:

```
[CLUSTER]
HOST=<your_host>
DB_NAME=<your_db_name>
DB_USER=<your_db_user>
DB_PASSWORD=<your_db_password>
DB_PORT=<your_db_port>

[IAM_ROLE]
ARN=<your_iam_role_arn>

[S3]
LOG_DATA='s3://udacity-dend/log_data'
LOG_JSONPATH='s3://udacity-dend/log_json_path.json'
SONG_DATA='s3://udacity-dend/song_data'

```

To execute the ETL on an existing cluster from the command line, navigate to the root of the project and run the following commands

```
python3 create_tables.py
python3 etl.py
```