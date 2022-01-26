from airflow.models import DAG
from datetime import datetime
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
        )
    '''
    )
