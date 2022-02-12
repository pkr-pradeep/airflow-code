import csv
import json
import os
from datetime import datetime
from zipfile import ZipFile

import pandas as pd
from airflow import DAG
from airflow.hooks.filesystem import FSHook
from airflow.models import Variable, XCom
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.db import provide_session
from jinja2 import Template

date_var = Variable.set("todays_date", datetime.now().strftime("%d%m%Y"))
date_today = Variable.get("todays_date")

default_args = {
    'start_date': datetime(2022, 1, 31),
    'retries': 3,
    'timeout': 60
}


def hook_the_db(**context):
    sqlite_hook = SqliteHook(sqlite_conn_id='db_sqlite')
    conn = sqlite_hook.get_conn()
    sql = '''
        SELECT count(*) as count FROM sqlite_master WHERE type='table' AND name='covid_user_travel_info';
        '''
    value = pd.read_sql_query(sql, conn)
    df = pd.DataFrame(value)
    is_exist = df["count"].values[0]
    if is_exist == 0:
        return 'creating_table'
    return 'store_data_to_xcoms'


def hook_the_data(**context):
    hook = FSHook("conn_filesensor_Odisha")
    full_path = [os.path.join(hook.get_path(), f'Covid_{state}_{date_today}.txt')
                 for state in ['Odisha', 'Gujarat', 'UttarPradesh']]
    print(full_path)
    for path in full_path:
        if os.path.isfile(path):
            with open(path) as file:
                base = os.path.basename(path)
                d = json.loads(file.read())
                context['ti'].xcom_push(key=os.path.splitext(base)[0], value=d)


def pull_parse_state_data(**context):
    list_state = ['Odisha', 'Gujarat', 'UttarPradesh']
    data_file = open("data/processed_travel_data.csv", "w+")
    csv_writer = csv.writer(data_file)
    count = 0
    for state in list_state:
     state_travel_data = context['ti'].xcom_pull(key=Template("Covid_{{state}}_{{today}}").render(today=date_today, state=state))
     print('state_travel_data', state_travel_data)
     if(not state_travel_data or len(state_travel_data) == 0):
      raise ValueError('value not found for ', state)
      #iterating each state travel data at covid
     for travel_data in state_travel_data:
      if count == 0:
        # Writing headers of CSV file
        header = travel_data.keys()
        csv_writer.writerow(header)
        count += 1
        # Writing data into CSV file
      csv_writer.writerow(travel_data.values())
    data_file.close()


def db_store():
    sqlite_hook = SqliteHook(sqlite_conn_id='db_sqlite')
    conn = sqlite_hook.get_conn()
    citizens_travel_data = pd.read_csv('data/processed_travel_data.csv')
    # write the data to a sqlite table
    citizens_travel_data.to_sql(
        'covid_user_travel_info', conn, if_exists='append', index=False)


def get_top_travellers(**context):
    citizens_travel_data = pd.read_csv('data/processed_travel_data.csv')
    report_headers = ['aadhar_no', 'count']
    data_file = open("data/top_travellers.csv", "w+")
    csv_writer = csv.writer(data_file)
    csv_writer.writerow(report_headers)
    userAndTravelcount = {}
    for i in range(len(citizens_travel_data['aadhar_no'])):
        if citizens_travel_data['aadhar_no'][i] not in userAndTravelcount.keys():
            userAndTravelcount[citizens_travel_data['aadhar_no'][i]] = 1
        else:
            count = userAndTravelcount.get(
                citizens_travel_data['aadhar_no'][i]) + 1
            userAndTravelcount[citizens_travel_data['aadhar_no'][i]] = count
    for user, travelcount in userAndTravelcount.items():
        if travelcount > 1:
            traveller_data = [user, travelcount]
            csv_writer.writerow(traveller_data)
    data_file.close()


def _archieve_and_move_the_file_after_processing():
    zipObj = ZipFile(Template("data/processed/Covid_traveldata_{{today}}.zip").render(today=date_today), 'w')
    list_state = ['Odisha', 'Gujarat', 'UttarPradesh']
    for state in list_state:
        t = Template("data/Covid_{{state}}_{{today}}.txt").render(today=date_today, state=state)
        t_move = Template("data/processed/Covid_{{state}}_{{today}}.txt").render(today=date_today, state=state)
        zipObj.write(t)
        os.replace(t, t_move)

@provide_session
def cleanup_xcom(session=None, **context):
        dag = context["dag"]
        dag_id = dag._dag_id 
        # It will delete all xcom of the dag_id
        session.query(XCom).filter(XCom.dag_id == dag_id).delete()


with DAG('covid_2019_travel_data_dag', 
          schedule_interval='@daily', 
          default_args=default_args,
          catchup=False) as dag:
    states = [
        FileSensor(
            task_id=f'covid_{state}',
            poke_interval=10,
            timeout=60,
            mode="reschedule",
            filepath=f'Covid_{state}_{date_today}.txt',
            fs_conn_id=f'conn_filesensor_{state}'
        ) for state in ['Odisha', 'Gujarat', 'UttarPradesh']]

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

    store_file_data_xcoms = PythonOperator(
        task_id='store_data_to_xcoms',
        python_callable=hook_the_data,
        provide_context=True
    )

    parse_file_data = PythonOperator(
        task_id='parse_file_data',
        python_callable=pull_parse_state_data,
        provide_context=True,
        trigger_rule='none_failed_or_skipped'
    )

    store = PythonOperator(
        task_id="store",
        python_callable=db_store
    )

    branch_py_op = BranchPythonOperator(
        task_id='branch_task',
        python_callable=hook_the_db,
        provide_context=True
    )

    op_top_travellers = PythonOperator(
        task_id="t_top_travellers",
        python_callable=get_top_travellers,
        provide_context=True
    )

    op_archieve_todays_file = PythonOperator(
        task_id = 't_archieve_file',
        python_callable= _archieve_and_move_the_file_after_processing
    )

    op_cleanup_xcoms =  PythonOperator(
        task_id="clean_xcom",
        python_callable = cleanup_xcom,
        provide_context=True
    )

    states >> branch_py_op >> [creating_table, store_file_data_xcoms] >> parse_file_data >> store >> op_top_travellers >> op_archieve_todays_file >> op_cleanup_xcoms
