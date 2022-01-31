import glob
import shutil
from tempfile import NamedTemporaryFile
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.exceptions import AirflowSensorTimeout
from datetime import datetime
from airflow.hooks.filesystem import FSHook
import os
from airflow.operators.email import EmailOperator

default_args = {
    'start_date': datetime(2022, 1, 31)
}

def hook_the_data(**context):
    hook = FSHook("conn_filesensor_Odisha");
    full_path=[os.path.join(hook.get_path(), f'Covid_{state}.txt') 
                for state in ['Odisha', 'Gujarat', 'UttarPradesh']]
    print(full_path)
    for path in full_path:
        if os.path.isfile(path):
            print('Found File %s last modified: %s', str(path))

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

with DAG('sense_file_dag', schedule_interval='@daily', default_args=default_args, catchup=False) as dag:
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
    states >> process >> store >> print_data