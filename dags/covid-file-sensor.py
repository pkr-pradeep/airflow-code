import os
import json
from datetime import datetime
from tempfile import NamedTemporaryFile

from airflow import DAG
from airflow.exceptions import AirflowSensorTimeout
from airflow.hooks.filesystem import FSHook
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from airflow.operators.email import EmailOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from pandas import json_normalize

default_args = {
    'start_date': datetime(2022, 1, 31)
}


def hook_the_db(**context):
    db_hook = SqliteHook("")


def hook_the_data(**context):
    hook = FSHook("conn_filesensor_Odisha")
    full_path = [os.path.join(hook.get_path(), f'Covid_{state}.txt')
                 for state in ['Odisha', 'Gujarat', 'UttarPradesh']]
    print(full_path)
    for path in full_path:
        if os.path.isfile(path):
            with open(path) as file:
                base = os.path.basename(path)
                d = json.loads(file.read())
                context['ti'].xcom_push(key=os.path.splitext(base)[0], value=d)


def pull_state_data(**context):
    list_state = ['Odisha', 'Gujarat', 'UttarPradesh']
    for state in list_state:
     state_travel_data = context['ti'].xcom_pull(key=state)
     if not len(state_travel_data):
      raise ValueError('value not found for ', state)
     # Convert string to Python dict
     travel_list = json.loads(state_travel_data)
     for x in range(len(travel_list)):
      processed_user = json_normalize({
          'aadhar_no': travel_list[x]['aadhar_no'],
          'travel_date' : travel_list[x]['travel_date'],
          'mode' : travel_list[x]['mode'],
          'state' : state
      })
      print(processed_user)

def data_process():
    pass

def db_store():
    pass


def _failure_callback(context):
    if isinstance(context['exception'], AirflowSensorTimeout):
        print(context)
    print("Sensor timed out")


def m_sent_email_alert(**context):
    print('preparing to send mail')
    with NamedTemporaryFile(mode='w+', suffix=".txt") as file:
        file.write("Hello World")

        email_op = EmailOperator(
            task_id='send_email',
            to="pradeep.rout@impetus.com",
            subject="Travellers data missing [Covid19]",
            html_content="Please Import the document in appropriate the folder",
            files=[file.name]
        )
        email_op.execute(context)


with DAG('covid_2019_travel_data_dag', schedule_interval='@daily', default_args=default_args, catchup=False) as dag:

    creating_table = SqliteOperator(
        task_id='creating_table',
        sqlite_conn_id='db_sqlite',
        sql='''
            CREATE TABLE covid_user_travel_info (
            aadhar_no INT,
            travel_date DATETIME,
            mode TEXT,
            state TEXT
            );
            '''
    )

    states = [
        FileSensor(
            task_id=f'covid_{state}',
            retries=3,
            poke_interval=10,
            timeout=60,
            mode="reschedule",
            on_failure_callback=m_sent_email_alert,
            filepath=f'Covid_{state}.txt',
            fs_conn_id=f'conn_filesensor_{state}'
        ) for state in ['Odisha', 'Gujarat', 'UttarPradesh']]

    process = PythonOperator(
        task_id="process",
        python_callable=data_process
    )

    store = PythonOperator(
        task_id="store",
        python_callable=db_store
    )

    print_data = PythonOperator(
        task_id='print_file_task',
        python_callable=hook_the_data,
        provide_context=True
    )

    """ email_op_python = PythonOperator(
        task_id="send_email_alert", python_callable=build_email, provide_context=True, dag=dag
    )
     """
    creating_table >> states >> process >> store >> print_data
