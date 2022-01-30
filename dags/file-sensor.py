from airflow.models import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.exceptions import AirflowSensorTimeout

with DAG(
        dag_id='sense_file',
        schedule_interval='@daily',
        start_date='2022-01-31',
        catchup=False) as dag:

    def _failure_callback(context):
        if isinstance(context['exception'], AirflowSensorTimeout):
            print(context)
        print("Sensor timed out")

    wait_for_file_op = FileSensor(
        task_id='t_waiting_file',
        poke_interval=100,
        timeout=60*7,
        mode='reschedule',
        retries=3,
        on_failure_callback=_failure_callback
    )
