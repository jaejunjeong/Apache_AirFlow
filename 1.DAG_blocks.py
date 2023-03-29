# import the libraries
from datetime import timedelta
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to write tasks!
from airflow.operators.bash_operator import BashOperator
# This makes scheduling easy
from airflow.utils.dates import days_ago

#defining DAG arguments

# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'Jaejun Jeong',
    'start_date': days_ago(0),
    'email': ['Github@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

##Setting Mean
#the owner name,
#when this DAG should run from: days_age(0) means today,
#the email address where the alerts are sent to,
#whether alert must be sent on failure,
#whether alert must be sent on retry,
#the number of retries in case of failure, and
#the time delay between retries.

# define the DAG
dag = DAG(
    dag_id='sample-etl-dag',
    default_args=default_args,
    description='Sample ETL DAG using Bash',
    schedule_interval=timedelta(days=1),
)

# define the tasks

download = BashOperator(
    task_id='download',
    bash_command='wget "eg_website/web-server-access-log.txt"',
    dag=dag,
)

# define the first task named extract
extract = BashOperator(
    task_id='extract',
    bash_command='cut -d"#" -f1,4 web-server-access-log.txt > /home/project/airflow/dags/extracted.txt',
    dag=dag,
)

# define the second task named transform
transform = BashOperator(
    task_id='transform',
    bash_command='tr "[a-z]" "[A-Z]" < /home/project/airflow/dags/extracted.txt > /home/project/airflow/dags/capitalized.txt'
    dag=dag,
)

# define the third task named load

load = BashOperator(
    task_id='load',
    bash_command='zip log.zip capitalized.txt',
    dag=dag,
)

# task pipeline
download >> extract >> transform >> load
