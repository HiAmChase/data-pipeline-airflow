import uuid
import json
from datetime import datetime

from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable

import config
from args import default_args

LEAGUES = ["PL", "BL1"]
ENDPOINT = "/competitions/{league}/matches?status=FINISHED"


def get_endpoint():
    season = Variable.get("season", default_var=None)
    return ENDPOINT if season is None else ENDPOINT + f"&season={season}"


def to_uuid(date, home_team, away_team):
    concat_str = (date + home_team + away_team).replace(" ", "")
    return uuid.uuid5(uuid.NAMESPACE_DNS, concat_str)


def generate_http_operator(next_task):
    for league in LEAGUES:
        endpoint = get_endpoint().format(league=league)
        is_matches_api_ready = HttpSensor(
            task_id=f"check_{league.lower()}_api_ready",
            http_conn_id=config.FOOTBALL_DATA_CONN_ID,
            endpoint=endpoint,
            headers={"X-Auth-Token": config.FOOTBALL_DATA_AUTH_TOKEN},
        )
        extract_matches_data = SimpleHttpOperator(
            task_id=f"extract_{league.lower()}_data",
            http_conn_id=config.FOOTBALL_DATA_CONN_ID,
            endpoint=endpoint,
            headers={"X-Auth-Token": config.FOOTBALL_DATA_AUTH_TOKEN},
            method="GET",
            response_filter=lambda r: json.loads(r.text),
            log_response=True,
        )

        is_matches_api_ready >> extract_matches_data >> next_task


def generate_query_data(matches_data):
    data_queries = [
        f"""
            (
                '{match["match_id"]}', '{match["season"]}', {match["week"]}, '{match["date"]}',
                '{match["home_team"]}', {match["home_score"]}, '{match["away_team"]}',
                {match["away_score"]}, '{match["final_result"]}', '{match["division"]}'
            )
        """
        for match in matches_data
    ]
    return ", ".join(data_queries)


def transform_matches_data(task_instance):
    matches_data = []
    matches_id_stored = task_instance.xcom_pull(task_ids="retrive_match_data")
    matches_id_stored = [item for sublist in matches_id_stored for item in sublist]
    for league in LEAGUES:
        data = task_instance.xcom_pull(task_ids=f"extract_{league.lower()}_data")
        season = data.get("filters")["season"]
        matches = data.get("matches", [])
        for match in matches:
            home_team = match["homeTeam"]["name"]
            away_team = match["awayTeam"]["name"]
            date = datetime.strptime(match["utcDate"], "%Y-%m-%dT%H:%M:%SZ").strftime(
                "%Y-%m-%d"
            )
            match_id = to_uuid(date, home_team, away_team)
            if str(match_id) in matches_id_stored:
                continue
            home_score = match["score"]["fullTime"]["home"]
            away_score = match["score"]["fullTime"]["away"]
            final_result = (
                "H"
                if home_score > away_score
                else "A"
                if away_score < home_score
                else "D"
            )
            match_data = {
                "match_id": match_id,
                "season": season,
                "week": match["matchday"],
                "date": date,
                "home_team": home_team,
                "home_score": home_score,
                "away_team": away_team,
                "away_score": away_score,
                "final_result": final_result,
                "division": league,
            }
            matches_data.append(match_data)
    if len(matches_data) == 0:
        return

    insert_query = f"""
        insert into match (
            id, season, match_day, date, home_team, home_team_goals,
            away_team, away_team_goals, final_result, division
        )
        values
            {generate_query_data(matches_data)}
    """
    postgres_hook = PostgresHook(postgres_conn_id=config.POSTGRES_CONN_ID)
    postgres_hook.run(insert_query)


with DAG(
    dag_id="matches_dag",
    default_args=default_args,
    schedule_interval="@yearly",
    catchup=False,
) as dag:
    season = Variable.get("season", default_var=None)
    if season is not None:
        select_query = f"select id from match where season='{season}'"
    else:
        select_query = "select id from match"

    retrive_data = PostgresOperator(
        task_id="retrive_match_data",
        sql=select_query,
        postgres_conn_id=config.POSTGRES_CONN_ID,
    )
    transform_match = PythonOperator(
        task_id="transform_matches_data", python_callable=transform_matches_data
    )

    retrive_data >> transform_match
    generate_http_operator(transform_match)
