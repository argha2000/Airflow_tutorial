from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datacleaner import data_cleaner
from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.email_operator import EmailOperator

yesterday_date = datetime.strftime(datetime.today() - timedelta(1),'%Y-%m-%d')

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

with DAG("store_DAG", default_args=default_args, schedule_interval='@daily',catchup=False,template_searchpath=["/usr/local/airflow/sql_files"]) as dag:

        t1 = BashOperator(task_id="check_file_exists", bash_command="shasum /usr/local/airflow/store_files_mysql/raw_store_transactions.csv", retry = 2,retry_delay=timedelta(seconds=15))

        t2 = PythonOperator(task_id = "clean_raw_csv",python_callable = data_cleaner)

        t3 = MySqlOperator(task_id = "create_table",mysql_conn_id = "mysql_conn",sql="create_table.sql")

        t4 =  MySqlOperator(task_id = "load_into_table",mysql_conn_id = "mysql_conn",sql="insert_into_table.sql")

        t5 = BashOperator(task_id = "move_file1",bash_command="mv /usr/local/airflow/store_files_mysql/transfer_to_business_files/location_wise_profit.csv /usr/local/airflow/store_files_mysql/archive/location_wise_profit_%s.csv" %yesterday_date)

        t6 = BashOperator(task_id = "move_file2",bash_command="mv /usr/local/airflow/store_files_mysql/transfer_to_business_files/store_wise_profit.csv /usr/local/airflow/store_files_mysql/archive/store_wise_profit_%s.csv"% yesterday_date)

        t7 = MySqlOperator(task_id = "select_from_table",mysql_conn_id = "mysql_conn",sql="select_from_table.sql")

        t8 = EmailOperator(task_id = "send_daily_run_email",to="example@gmail.com",subject="DAG Execution Successfull",html_content="<h1>Congratulations! Your DAG executed successfully</h1>")

        t1 >> t2 >> t3 >>t4 >>  [t5,t6] >> t7 >> t8