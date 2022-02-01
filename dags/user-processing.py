import imp
import json
import pandas as pd
from datetime import datetime
from urllib import response

from airflow.models import DAG
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator

default_args = {
    'start_date': datetime(2022, 1, 26)
}

def hook_the_db(**context):
    sqlite_hook = SqliteHook(sqlite_conn_id='db_sqlite')
    conn = sqlite_hook.get_conn()
    sql='''
        SELECT count(*) as count FROM sqlite_master WHERE type='table' AND name='user_travel_info';
        '''
    value = pd.read_sql_query(sql, conn)
    df = pd.DataFrame(value)
    is_exist = df["count"].values[0];
    if is_exist == 0:
        return 'creating_table'
    return 'is_api_available'


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
        log_response=True,
        trigger_rule='none_failed_or_skipped'
    )

    branch_py_op = BranchPythonOperator(
        task_id='branch_task',
        python_callable=hook_the_db,
        provide_context=True
    )

    branch_py_op >> [creating_table, is_api_availble] >> extract_user
