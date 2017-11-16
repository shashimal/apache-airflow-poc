from airflow import DAG
from airflow.operators import EmailOperator
from airflow.operators import BashOperator
from airflow.operators.sensors import S3KeySensor
from airflow.operators.sensors import SqlSensor
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators import SlackAPIPostOperator
from airflow.operators import DataLoadOperator, UpdateDownloadStatusSensor
from datetime import datetime, timedelta


def update_download_count(**kwargs):
    kwargs['ti'].xcom_push(key='new_download', value=10)


default_args = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'start_date': datetime(2017, 10, 31),
    'email': ['duleendra@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}


dag = DAG('airflow_poc_final', schedule_interval='12 00 * * *', default_args=default_args)

# Initiate the workflow
start = DummyOperator(task_id='start', retries=1, dag=dag)

# S3 sensor check whether any matching keys are available
s3_sensor = S3KeySensor(
    task_id='s3_sensor',
    timeout=18 * 60 * 60,
    poke_interval=20,
    soft_fail=True,
    bucket_key='airflow-poc/*.csv',
    wildcard_match=True,
    bucket_name='report-service-test',
    s3_conn_id='conn_S3',
    dag=dag)

# Download the files from S3
bash_command_download = '/Users/dwarakagoda/Ops/Airflow/scrtips/download_s3_csv.sh '
download_s3_csv = BashOperator(task_id='download_s3_csv', bash_command=bash_command_download, dag=dag)

# Check newly downloaded files from S3
check_newly_downloaded_files = SqlSensor(
    task_id='check_newly_downloaded_files',
    poke_interval=30,
    timeout=3200,
    sql="""SELECT count(id) FROM airflow_pipeline.download_status WHERE status='NEW' """,
    conn_id='mysql_test',
    dag=dag)

# Update download count
update_download_count = PythonOperator(
    task_id='update_download_count',
    python_callable=update_download_count,
    provide_context=True,
    dag=dag)

# Download summary notification
download_summary_notification = EmailOperator(
        task_id='download_summary_notification',
        to="shashimald@gmail.com",
        subject="Download Summary {{ task_instance.xcom_pull(task_ids='update_download_count', key='new_download') }}",
        html_content="Download Summary {{ task_instance.xcom_pull(task_ids='update_download_count', key='new_download') }}",
        trigger_rule='all_done',
        dag=dag)


# Data loading task
data_load = DataLoadOperator(operator_param='This is a test.', task_id='data_load', dag=dag)

# Email will be sent after the data loading
email_notification = EmailOperator(
    task_id='email_notification',
    to="shashimald@gmail.com",
    # params={'map': map},
    subject="Data Loading Status",
    html_content="Data Loading Status",
    trigger_rule='all_done',
    dag=dag)

# Slack message will be sent after the data loading
slack_notification = SlackAPIPostOperator(
    task_id='slack_notification',
    channel="#data-team",
    token='xoxp-265033213506-265171437413-264526426977-068c91e535e211028ca4c0dd896cd57c',
    text='Hello From Airflow',
    trigger_rule='all_done', dag=dag)


s3_sensor.set_upstream(start)
download_s3_csv.set_upstream(s3_sensor)
check_newly_downloaded_files.set_upstream(download_s3_csv)
update_download_count.set_upstream(check_newly_downloaded_files)
download_summary_notification.set_upstream(update_download_count)
data_load.set_upstream(check_newly_downloaded_files)
email_notification.set_upstream(data_load)
slack_notification.set_upstream(data_load)
