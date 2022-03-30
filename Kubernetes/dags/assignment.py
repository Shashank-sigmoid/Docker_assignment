from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

from db_connection import create_connection
from read_table import read_table_data

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2022, 3, 30),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}


def check():
    print("Test statement to check if dag is running properly...")


dag = DAG("assignment", default_args=default_args, schedule_interval=timedelta(1))

t1 = PythonOperator(task_id="Check_DAG", python_callable=check, dag=dag)

t2 = PythonOperator(task_id="Create_table", python_callable=create_connection, dag=dag)

t3 = PythonOperator(task_id="Print_data", python_callable=read_table_data, dag=dag)

t1 >> t2 >> t3

