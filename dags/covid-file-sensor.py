from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.exceptions import AirflowSensorTimeout
from datetime import datetime
default_args = {
    'start_date': datetime(2022, 1, 31)
}
def data_process():
    pass
def db_store():
    pass
def _failure_callback(context):
    if isinstance(context['exception'], AirflowSensorTimeout):
        print(context)
    print("Sensor timed out")
with DAG('my_dag', schedule_interval='@daily', default_args=default_args, catchup=False) as dag:
    states = [
        FileSensor(
        task_id=f'covid_{state}',
        poke_interval=60,
        timeout=60 * 30,
        mode="reschedule",
        on_failure_callback=_failure_callback,
        filepath=f'covid_{state}.txt',
        fs_conn_id=f'conn_filesensor_{state}'
    ) for state in ['Odisha', 'Gujarat', 'UttarPradesh']]

    process = PythonOperator(
        task_id="process",
        python_callable=data_process,
    )
    store = PythonOperator(
        task_id="store",
        python_callable=db_store,
    )
    states >> process >> store
