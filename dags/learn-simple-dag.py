from logging import exception
from multiprocessing.sharedctypes import Value
from airflow.models import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.dates import timedelta
import random

args = {
    'start_date': datetime(2022, 1, 29)
}

dag = DAG(dag_id='learn_dag', description='Simple DAG',
          default_args=args, schedule_interval='@daily', catchup=False)


def my_function():
  print("Hello World! First Operator")


def m_branch_select(**context):
    if random.random() > 0.5:
        return 'pull_xcom_hi'
    return 'pull_xcom_hello'


def m_raise_exception():
    val = random.random()
    if(val < 0.7):
        raise Exception("less than 0.7, val = %.2f", val)
    else:
        print("I am okay! %f", val)


def m_xcom_push(**context):
    r_val = random.random()
    context['ti'].xcom_push(key='r_val', value=r_val)
    print('xcom pushed')


def m_xcom_pull_hello(**context):
    random_value = context['ti'].xcom_pull(key='r_val')
    print(f'hello, xcom pulled {str(random_value)}')


def m_xcom_pull_hi(**context):
    random_value = context['ti'].xcom_pull(key='r_val')
    print(f'hi, xcom pulled {str(random_value)}')


with dag:

    branch_py_op = BranchPythonOperator(
        task_id='branch_task',
        python_callable=m_branch_select,
        provide_context=True
    )

    print_message = PythonOperator(
        task_id='hello_world',
        python_callable=my_function,
    )

    #This is for retries and gap between each retry once it throws exception or task failed
    retry_op = PythonOperator(
        task_id='retry_task',
        python_callable=m_raise_exception,
        retries=3,
        retry_delay=timedelta(seconds=1)
    )

    xcom_push_op = PythonOperator(
        task_id='push_xcom',
        python_callable=m_xcom_push,
        provide_context=True
    )

    xcom_pull_op_hi = PythonOperator(
        task_id='pull_xcom_hi',
        python_callable=m_xcom_pull_hi,
    )

    xcom_pull_op_hello = PythonOperator(
        task_id='pull_xcom_hello',
        python_callable=m_xcom_pull_hello,
    )

    print_message >> retry_op >> xcom_push_op >> branch_py_op >> [xcom_pull_op_hi, xcom_pull_op_hello]
