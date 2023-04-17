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
ETL_toll_data = DAG(
    dag_id='ETL_toll_data',
    default_args=default_args,
    description='Apache Airflow Final Assignment',
    schedule_interval=timedelta(days=1),
)

# define the tasks

unzip_data = BashOperator(
    task_id='unzip_data',
    bash_command='tar -xvzf /home/project/airflow/dags/finalassignment/tolldata.tgz -C /home/project/airflow/dags/finalassignment',
    dag=ETL_toll_data,
)

extract_data_from_csv = BashOperator(
    task_id='extract_data_from_csv',
    bash_command='cut -d"," -f1-4 vehicle-data.csv > csv_data.csv',
    dag=ETL_toll_data,
)

extract_data_from_tsv = BashOperator(
    task_id='extract_data_from_tsv',
    bash_command='cut -d$"\t" -f5-7 tollplaza-data.tsv | tr "\t" "," > tsv_data.csv',
    dag=ETL_toll_data,
)

extract_data_from_fixed_width = BashOperator(
    task_id='extract_data_from_fixed_width',
    bash_command='cut -c59- payment-data.txt | tr " " "," > fixed_width_data.csv',
    dag=ETL_toll_data,
)

consolidate_data = BashOperator(
    task_id='consolidate_data',
    bash_command='paste csv_data.csv tsv_data.csv fixed_width_data.csv > extracted_data.csv',
    dag=ETL_toll_data,
)

transform_data = BashOperator(
    task_id='transform_data',
    bash_command='awk "$5 = toupper($5)" < extracted_data.csv > transformed_data.csv',
    dag=ETL_toll_data,
)

# task pipeline
unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data
