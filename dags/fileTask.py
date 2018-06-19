import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import timedelta
import datetime

# these args will get passed on to each operator
# you can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(2),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'adhoc':False,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function
}

dag = DAG(
    'fileTask',
    default_args=default_args,
    description='A simple file task',
    schedule_interval="15 08 * * *",
    dagrun_timeout=timedelta(minutes=1))


# t1 is the task to create a file

table_name = 'Test_File'
start_date = datetime.date.today()
cur_tmpstp = start_date.strftime('%Y_%m_%d')


output_filename = table_name + "_" + cur_tmpstp + ".txt"

landing_path = "/home/charanjiths/airflow"


task_1 = BashOperator(
    task_id='task_1',
    bash_command=" cp %s %s/ " %(output_filename, landing_path, ),
    dag=dag)


