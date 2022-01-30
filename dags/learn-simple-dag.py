from logging import exception
from airflow.models import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.utils.dates import timedelta
import random

args = {
    'start_date' : datetime(2022,1,29)
}

dag = DAG(dag_id='learn_dag', description='Simple DAG', default_args=args, schedule_interval = '@daily')

def my_function():
  print("Hello from a function")

def raise_exception():
    val = random.random()
    if(val < 0.7):
        raise Exception("less than 0.7, val = %.2f",val)
    else:
        print("I am okay! %f",val);

with dag:
    print_message = PythonOperator(
        task_id = 'print_message',
        python_callable= raise_exception,
        retries=3,
        retry_delay=timedelta(seconds=1)
    )

    print_message2 = PythonOperator(
        task_id = 'print_message2',
        python_callable= my_function,
    )

    print_message >> print_message2