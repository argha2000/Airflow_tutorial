from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 12, 13),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

dag = DAG("Assignment1", default_args=default_args, schedule_interval=timedelta(1))

# Task 1: Check if directory exists on the Windows filesystem (through WSL2)
t1 = BashOperator(
    task_id="check_directory_exists",
    bash_command="""if [ ! -d /mnt/c/Airflow_tutorial/test_dir ]; then
        echo "Directory does not exist."
        exit 0
    else
        echo "Directory exists."
        exit 1
    fi""",
    dag=dag
)

t2 = BashOperator(
    task_id="make_directory",
    bash_command="mkdir -p /mnt/c/Airflow_tutorial/test_dir",  
    dag=dag
)

t1 >> t2


