from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datacleaner import data_cleaner
from airflow.operators.mysql_operator import MySqlOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 12, 22),
    "retries": 1,
    "retry_delay": timedelta(seconds=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG("store_DAG", default_args=default_args, schedule_interval='@daily',catchup=False,template_searchpath=["/usr/local/airflow/sql_files"])

t1 = BashOperator(task_id="check_file_exists", bash_command="shasum /usr/local/airflow/store_files_mysql/raw_store_transactions.csv", retry = 2,retry_delay=timedelta(seconds=15),dag = dag)

t2 = PythonOperator(task_id = "clean_raw_csv",python_callable = data_cleaner,dag = dag)

t3 = MySqlOperator(task_id = "create_table",mysql_conn_id = "mysql_conn",sql="create_table.sql",dag = dag)

t4 =  MySqlOperator(task_id = "load_into_table",mysql_conn_id = "mysql_conn",sql="insert_into_table.sql",dag = dag)

t5 = BashOperator(
    task_id="remove_previous_files",
    bash_command="""
    echo "Current user:"
    whoami
    echo "Current permissions:"
    ls -la /usr/local/airflow/store_files_mysql/transfer_to_business_files/
    echo "Attempting to remove files..."
    rm  /usr/local/airflow/store_files_mysql/transfer_to_business_files/*.csv && echo "Files removed successfully" || echo "Failed to remove files && exit 1"
    """,
    dag=dag
)
t6 = MySqlOperator(task_id = "select_from_table",mysql_conn_id = "mysql_conn",sql="select_from_table.sql",dag = dag)


t1 >> t2 >> t3 >>t4 >> t5 >> t6