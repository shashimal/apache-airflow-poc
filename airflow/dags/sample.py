from airflow import DAG
from airflow.operators import DummyOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'start_date': datetime.now() - timedelta(seconds=10),
    'retries': 0
}

dag = DAG('sample', default_args=default_args, start_date=datetime.now() - timedelta(seconds=10))

op = DummyOperator(task_id='dummy', dag=dag)
