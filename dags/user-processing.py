import imp
import json
from datetime import datetime
from urllib import response

from airflow.models import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.sqlite.operators.sqlite import SqliteOperator

default_args = {
    'start_date': datetime(2022, 1, 26)
}

with DAG('user_processing', schedule_interval='@daily',
    default_args=default_args,
        catchup=False) as dag:

    creating_table = SqliteOperator(
        task_id='creating_table',
        sqlite_conn_id='db_sqlite',
        sql='''
            CREATE TABLE user_travel_info (
            aadhar_no INT PRIMARY KEY,
            travel_date DATETIME,
            mode TEXT
            );
            '''
    )

    is_api_availble = HttpSensor(
        task_id = 'is_api_available',
        http_conn_id = 'user_api',
        endpoint='traveldata/'
    )

    extract_user = SimpleHttpOperator(
        task_id = 'extract_user',
        http_conn_id = 'user_api',
        endpoint='traveldata/',
        method='GET',
        response_filter=lambda response: json.loads(response.text),
        log_response=True
    )

    creating_table << is_api_availble >> extract_user
