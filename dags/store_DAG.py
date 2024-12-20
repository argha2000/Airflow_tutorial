from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datacleaner import data_cleaner
from airflow.operators.mysql_operator import MySqlOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 12, 21),
    "retries": 1,
    "retry_delay": timedelta(seconds=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG("store_DAG", default_args=default_args, schedule_interval='@daily',catchup=False,template_searchpath=["usr/local/airflow/sql_files"])

t1 = BashOperator(task_id="check_file_exists", bash_command="shasum ~/store_files_airflow/raw_store_transactions.csv", retry = 2,retry_delay=timedelta(seconds=15),dag = dag)

t2 = PythonOperator(task_id = "clean_raw_csv",python_callable = data_cleaner,dag = dag)

t3 = MySqlOperator(task_id = "create_and_load_into_table",mysql_conn_id = "mysql_conn",sql="create_table.sql")

t1 >> t2 >> t3