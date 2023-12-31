# Data Pipelin Airflow

Build Data Pipelines with Apache Airflow

### I. DAGs Overview

#### 1. Extract weather data from Open Weather Map API

The ETL process that can extract current weather data from Open Weather Map API, transform the data and load the data into an S3 bucket using Apache Airflow

The DAGs graph has the following structure:
![weather-image](img/weather_dag.png)

Describe the function of each task:

- `is_weather_api_ready`: Check if the Open Weather Map API is working or not
- `extract_weather_data`: Send request to Open Weather Map API to get data
- `transform_load_weather_data`: Transform data from Open Weather Map API and add to S3 bucket
- `check_upstream_success`: Check all the upstream is success or not
- `send_email_report`: Get information from task `check_upstream_success` to know whether the DAG run is success or not and send email report to owner

#### 2. Get football matches results from Football Data

Extract the football matches from [Football Data](https://www.football-data.org/), transform and load it into target database using PostgreSQL

The DAGs graph:

![football-image](img/matches_dag.png)

Describe the function of each task:

- `check_pl_api_ready` and `check_bl1_api_ready`: Check whether the API for Premier League and Bundesliga matches works or not
- `extract_pl_data` and `extract_bl1_data`: Send request to get the matches data
- `retrive_match_data`: Get all matches currently exists in database
- `transform_matches_data`: Transform data from API and validate if the matches is exists in database or not. The validate rules is calculate by converting to uuid by 3 values (date, home_team, away_team). Finally insert all available matches into database
- `check_upstream_success`: Check all the upstream is success or not
- `send_email_report`: Get information from task `check_upstream_success` to know whether the DAG run is success or not and send email report to owner

The table `match` in PostgreSQL was created by script:

```sql
CREATE TABLE public."match" (
	season varchar(8) NOT NULL,
	match_day int4 NOT NULL,
	"date" date NOT NULL,
	home_team varchar(256) NOT NULL,
	home_team_goals int4 NOT NULL,
	away_team varchar(256) NOT NULL,
	away_team_goals int4 NOT NULL,
	final_result varchar(3) NOT NULL,
	division varchar(5) NOT NULL,
	id uuid NOT NULL,
	CONSTRAINT match_pkey PRIMARY KEY (id)
);
```

### II. Setup Configuration and Variable

#### 1. Create database to store Airflow metadata

You need to create a database and a database user that Airflow will use to access this database

```sql
CREATE DATABASE airflow;
CREATE USER airflow WITH PASSWORD 'airflow';
GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;
-- PostgreSQL 15 requires additional privileges:
USE airflow;
GRANT ALL ON SCHEMA public TO airflow;
```

You can update to any value you want. But remember to update the airflow database connection in `docker-compose.yml` with 2 variables: `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN`, `AIRFLOW__CORE__SQL_ALCHEMY_CONN`

#### 2. Create airflow environment variables

Create new file `airflow.secret` with these variables:

```bash
AIRFLOW__SMTP__SMTP_USER=your_username
AIRFLOW__SMTP__SMTP_PASSWORD=your_password
AIRFLOW__SMTP__SMTP_MAIL_FROM=your_email
```

These variable are using for sending email through the Simple Mail Transfer Protocol (SMTP)

#### 3. Airflow Connection

From Airflow main screen, select **Admin** > **Connection** then add these following connection:

- ConnectionId: footbal_data_api
  - Host: https://api.football-data.org/v4
  - ConnectionType: HTTP
- ConnectionId: weathermap_api

  - Host: https://api.openweathermap.org
  - ConnectionType: HTTP

- ConnectionId: postgres_conn

  - Host: Your database Host
  - Port: Your database Port
  - ConnectionType: Postgres

- ConnectionId: s3_conn
  - Add aws_access_key_id
  - Add aws_secret_access_key
  - ConnectionType: Amazon Web Services

#### 4. Configuration

Add file `config.py` to folder `dags`

```python
AWS_CONN_ID = "s3_conn"
OPEN_WEATHER_BUCKET_NAME = "your-bucket-in-aws"
OPEN_WEATHER_HTTP_CONN_ID = "weathermap_api"
OPEN_WEATHER_API_KEY = "your-open-weather-api-key"
FOOTBALL_DATA_CONN_ID = "footbal_data_api"
FOOTBALL_DATA_AUTH_TOKEN = "your-football-auth-token"
POSTGRES_CONN_ID = "postgres_conn"
```
�